import inspect
import json
import multiprocessing
import queue
import os
import pprint
import pymysql
import traceback
from datetime import datetime
from time import sleep
import znc


class DispatchTimer(znc.Timer):
    def RunJob(self):
        while True:
            try:
                line = self.queue.get_nowait()
                self.put_irc(
                    line["user"],
                    line["network"],
                    line["window"],
                    line["type"],
                    line["nick"],
                    line["message"],
                )
            except queue.Empty:
                break
            except Exception as e:
                with self.internal_log.error() as log:
                    log.write("DispatchTimer crash:\n")
                    log.write(traceback.format_exc())
                break


class zlog_sql(znc.Module):
    description = "Logs all channels to a MySQL or SQLite database."
    module_types = [znc.CModInfo.GlobalModule]

    wiki_page = "ZLog_SQL"

    hook_debugging = False

    def put_irc(self, user, network, window, mtype, target, message):
        self.internal_log.debug().write(
            "Dispatching message to user: {} network: {} window: {} type: {} target: {} message: {}\n".format(
                user, network, window, mtype, target, message
            )
        )
        query = znc.CZNC.Get().FindUser(user)
        self.internal_log.debug().write("Found user: {}\n".format(query is not None))
        if query is not None:
            query = query.FindNetwork(network)
            self.internal_log.debug().write("Found network: {}\n".format(query is not None))
            if query is not None:
                if window == target:
                    self.internal_log.debug().write("Target is window\n")
                    line = "{} {} :{}".format(self.types[mtype], window, message)
                else:
                    self.internal_log.debug().write("Target is different from window\n")
                    line = "{} {} :{}: {}".format(
                        self.types[mtype], window, target, message
                    )
                query.PutIRC(line)

    def OnLoad(self, args, message):
        """
        This module hook is called when a module is loaded.
        :rtype: bool
        :param args: The arguments for the modules.
        :param message: A message that may be displayed to the user after loading the module.
        :return: True if the module loaded successfully, else False.
        """
        self.types = {"msg": "PRIVMSG", "action": "ACTION"}
        self.log_queue = multiprocessing.Queue()
        self.reply_queue = multiprocessing.Queue()
        self.internal_log = InternalLog(self.GetSavePath())
        self.internal_log.debug().write(
            "Module loaded at: {} UTC\n".format(datetime.utcnow())
        )
        self.internal_log.debug().write("Path: {}\n".format(self.GetSavePath()))
        self.debug_hook()

        try:
            remote_db = PlanetscaleDatabase(path=self.GetSavePath())
            db = SQLiteDatabase(path=self.GetSavePath())
            self.processes = []
            worker = multiprocessing.Process(
                target=DatabaseProcess.worker_safe,
                args=(db, self.log_queue, self.internal_log),
            )
            worker.start()
            self.processes.append(worker)

            self.isDone = multiprocessing.Value("i", 0)
            poller = multiprocessing.Process(
                target=DatabaseProcess.poll_safe,
                args=(remote_db, self.reply_queue, self.isDone, self.internal_log),
            )
            poller.start()
            self.processes.append(poller)
            if remote_db is not None:
                syncer = multiprocessing.Process(
                    target=DatabaseProcess.sync_safe,
                    args=(db, remote_db, self.isDone, self.internal_log),
                )
                syncer.start()
                self.processes.append(syncer)
            timer = self.CreateTimer(
                DispatchTimer,
                interval=5,
                cycles=0,
                description="Message dispatch timer",
            )
            timer.internal_log = self.internal_log
            timer.queue = self.reply_queue
            timer.put_irc = self.put_irc
            return True
        except Exception as e:
            message.s = str(e)

            with self.internal_log.error() as target:
                target.write(
                    "Could not initialize module caused by: {} {}\n".format(
                        type(e), str(e)
                    )
                )
                target.write("Stack trace: " + traceback.format_exc())
                target.write("\n")

            return False

    def OnShutdown(self):
        # Terminate worker processes.
        self.log_queue.put(None)
        self.isDone.value = 1
        for proc in getattr(self, "processes", []):
            proc.join()

    def GetServer(self):
        pServer = self.GetNetwork().GetCurrentServer()

        if pServer is None:
            return "(no server)"

        sSSL = "+" if pServer.IsSSL() else ""
        return pServer.GetName() + " " + sSSL + pServer.GetPort()

    # GENERAL IRC EVENTS
    # ==================

    def OnIRCConnected(self):
        """
        This module hook is called after a successful login to IRC.
        :rtype: None
        """
        self.debug_hook()
        self.put_log("connect", self.GetServer())

    def OnIRCDisconnected(self):
        """
        This module hook is called when a user gets disconnected from IRC.
        :rtype: None
        """
        self.debug_hook()
        self.put_log("disconnect", self.GetServer())

    def OnBroadcast(self, message):
        """
        This module hook is called when a message is broadcasted to all users.
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log(
            "broadcast",
            str(message),
        )
        return znc.CONTINUE

    def OnRawMode(self, opNick, channel, modes, args):
        """
        Called on any channel mode change.
        This is called before the more detailed mode hooks like e.g. OnOp() and OnMode().
        :type opNick: const CNick &
        :type channel: CChan &
        :type modes: const CString &
        :type args: const CString &
        :rtype: None
        """
        self.debug_hook()
        sNick = opNick.GetNick() if opNick is not None else "Server"
        self.put_log(
            "rawmode",
            sNick + " sets mode: " + modes + " " + args,
            channel.GetName(),
            sNick,
        )

    def OnKick(self, opNick, kickedNick, channel, message):
        """
        Called when a nick is kicked from a channel.
        :type opNick: const CNick &
        :type kickedNick: const CString &
        :type channel: CChan &
        :type message: const CString &
        :rtype: None
        """
        self.debug_hook()
        self.put_log(
            "kick",
            kickedNick + " was kicked by " + opNick.GetNick() + " (" + message + ")",
            channel.GetName(),
            kickedNick,
        )

    def OnQuit(self, nick, message, channels):
        """
        Called when a nick quit from IRC.
        :type nick: const CNick &
        :type message: const CString &
        :type channels: std::vector<CChan*>
        :rtype: None
        """
        self.debug_hook()
        for channel in channels:
            self.put_log(
                "quit",
                nick.GetNick()
                + " ("
                + nick.GetIdent()
                + "@"
                + nick.GetHost()
                + ") ("
                + message
                + ")",
                channel.GetName(),
                nick.GetNick(),
            )

    def OnJoin(self, nick, channel):
        """
        Called when a nick joins a channel.
        :type nick: const CNick &
        :type channel: CChan &
        :rtype: None
        """
        self.debug_hook()
        self.put_log(
            "join",
            nick.GetNick() + " (" + nick.GetIdent() + "@" + nick.GetHost() + ")",
            channel.GetName(),
            nick.GetNick(),
        )

    def OnPart(self, nick, channel, message):
        """
        Called when a nick parts a channel.
        :type nick: const CNick &
        :type channel: CChan &
        :type message: const CString &
        :rtype: None
        """
        self.debug_hook()
        self.put_log(
            "part",
            nick.GetNick()
            + " ("
            + nick.GetIdent()
            + "@"
            + nick.GetHost()
            + ") ("
            + message
            + ")",
            channel.GetName(),
            nick.GetNick(),
        )

    def OnNick(self, oldNick, newNick, channels):
        """
        Called when a nickname change occurs.
        :type oldNick: const CNick &
        :type newNick: const CString &
        :type channels: std::vector<CChan*>
        :rtype: None
        """
        self.debug_hook()
        for channel in channels:
            self.put_log(
                "nick",
                oldNick.GetNick() + " is now known as " + newNick,
                channel.GetName(),
                oldNick.GetNick(),
            )

    def OnTopic(self, nick, channel, topic):
        """
        Called when we receive a channel topic change from IRC.
        :type nick: CNick &
        :type channel: CChan &
        :type topic: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log("topic", str(topic), channel.GetName(), nick.GetNick())
        return znc.CONTINUE

    # NOTICES
    # =======

    def OnUserNotice(self, target, message):
        """
        This module hook is called when a user sends a NOTICE message.
        :type target: CString &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        network = self.GetNetwork()
        if network:
            self.put_log("notice", str(message), str(target), network.GetCurNick())
        return znc.CONTINUE

    def OnPrivNotice(self, nick, message):
        """
        Called when we receive a private NOTICE message from IRC.
        :type nick: CNick &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log("notice", str(message), nick.GetNick(), nick.GetNick())
        return znc.CONTINUE

    def OnChanNotice(self, nick, channel, message):
        """
        Called when we receive a channel NOTICE message from IRC.
        :type nick: CNick &
        :type channel: CChan &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log("notice", str(message), channel.GetName(), nick.GetNick())
        return znc.CONTINUE

    # ACTIONS
    # =======

    def OnUserAction(self, target, message):
        """
        Called when a client sends a CTCP ACTION request ("/me").
        :type target: CString &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        pNetwork = self.GetNetwork()
        if pNetwork:
            self.put_log("action", str(message), str(target), pNetwork.GetCurNick())
        return znc.CONTINUE

    def OnPrivAction(self, nick, message):
        """
        Called when we receive a private CTCP ACTION ("/me" in query) from IRC.
        :type nick: CNick &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log("action", str(message), nick.GetNick(), nick.GetNick())
        return znc.CONTINUE

    def OnChanAction(self, nick, channel, message):
        """
        Called when we receive a channel CTCP ACTION ("/me" in a channel) from IRC.
        :type nick: CNick &
        :type channel: CChan &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log("action", str(message), channel.GetName(), nick.GetNick())
        return znc.CONTINUE

    # MESSAGES
    # ========

    def OnUserMsg(self, target, message):
        """
        This module hook is called when a user sends a PRIVMSG message.
        :type target: CString &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        network = self.GetNetwork()
        if network:
            self.put_log("msg", str(message), str(target), network.GetCurNick())
        return znc.CONTINUE

    def OnPrivMsg(self, nick, message):
        """
        Called when we receive a private PRIVMSG message from IRC.
        :type nick: CNick &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log("msg", str(message), nick.GetNick(), nick.GetNick())
        return znc.CONTINUE

    def OnChanMsg(self, nick, channel, message):
        """
        Called when we receive a channel PRIVMSG message from IRC.
        :type nick: CNick &
        :type channel: CChan &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log("msg", str(message), channel.GetName(), nick.GetNick())
        return znc.CONTINUE

    # LOGGING
    # =======

    def put_log(self, mtype, line, window="Status", nick=None):
        """
        Adds the log line to database write queue.
        """
        self.log_queue.put(
            {
                "created_at": datetime.utcnow().isoformat(),
                "user": (
                    self.GetUser().GetUserName() if self.GetUser() is not None else None
                ),
                "network": (
                    self.GetNetwork().GetName()
                    if self.GetNetwork() is not None
                    else None
                ),
                "window": window,
                "type": mtype,
                "nick": nick,
                "message": line.encode("utf8", "replace").decode("utf8"),
            }
        )

    # DEBUGGING HOOKS
    # ===============

    def debug_hook(self):
        """
        Dumps parent calling method name and its arguments to debug logfile.
        """

        if self.hook_debugging is not True:
            return

        frameinfo = inspect.stack()[1]
        argvals = frameinfo.frame.f_locals

        with self.internal_log.debug() as target:
            target.write("Called method: " + frameinfo.function + "()\n")
            for argname in argvals:
                if argname == "self":
                    continue
                target.write(
                    "    " + argname + " -> " + pprint.pformat(argvals[argname]) + "\n"
                )
            target.write("\n")


class DatabaseProcess:
    @staticmethod
    def worker_safe(db, log_queue: multiprocessing.Queue, internal_log) -> None:
        try:
            DatabaseProcess.worker(db, log_queue, internal_log)
        except Exception as e:
            with internal_log.error() as target:
                target.write(
                    "Unrecoverable exception in worker Process: {0} {1}\n".format(
                        type(e), str(e)
                    )
                )
                target.write("Stack trace: " + traceback.format_exc())
                target.write("\n")
            raise

    @staticmethod
    def worker(db, log_queue: multiprocessing.Queue, internal_log) -> None:
        db.connect()

        while True:
            item = log_queue.get()
            if item is None:
                break

            try:
                db.ensure_connected()
                db.insert_into(item, "logs")
            except Exception as e:
                sleep_for = 10

                with internal_log.error() as target:
                    target.write(
                        "Could not save to database caused by: {0} {1}\n".format(
                            type(e), str(e)
                        )
                    )
                    if "open" in dir(db.conn):
                        target.write("Database handle state: {}\n".format(db.conn.open))
                    target.write("Stack trace: " + traceback.format_exc())
                    target.write("Current log: ")
                    json.dump(item, target)
                    target.write("\n\n")
                    target.write("Retry in {} s\n".format(sleep_for))

                sleep(sleep_for)

                with internal_log.error() as target:
                    target.write("Retrying now.\n")
                    log_queue.put(item)

    @staticmethod
    def poll_safe(
        db,
        inbound_queue: multiprocessing.Queue,
        isDone: multiprocessing.Value,
        internal_log,
    ) -> None:
        try:
            DatabaseProcess.poll_worker(db, inbound_queue, isDone, internal_log)
        except Exception as e:
            with internal_log.error() as target:
                target.write(
                    "Unrecoverable exception in worker Process: {0} {1}\n".format(
                        type(e), str(e)
                    )
                )
                target.write("Stack trace: " + traceback.format_exc())
                target.write("\n")
            raise

    @staticmethod
    def poll_worker(
        db,
        inbound_queue: multiprocessing.Queue,
        isDone: multiprocessing.Value,
        internal_log,
    ) -> None:
        db.connect()

        while True:
            if isDone.value == 1:
                break
            sleep(5)
            try:
                db.ensure_connected()
                res = db.fetch_from()
                if res:
                    for row in res:
                        inbound_queue.put({
                            "user": row["user"],
                            "network": row["network"],
                            "window": row["window"],
                            "type": row["type"],
                            "nick": row["nick"],
                            "message": row["message"],
                        })
                        db.del_from(row["id"])
            except Exception as e:
                sleep_for = 10

                with internal_log.error() as target:
                    target.write(
                        "Could not read from database caused by: {0} {1}\n".format(
                            type(e), str(e)
                        )
                    )
                    if "open" in dir(db.conn):
                        target.write("Database handle state: {}\n".format(db.conn.open))
                    target.write("Stack trace: " + traceback.format_exc())
                    target.write("\n\n")
                    target.write("Retry in {} s\n".format(sleep_for))

                sleep(sleep_for)

                with internal_log.error() as target:
                    target.write("Retrying now.\n")

    @staticmethod
    def sync_safe(
        local_db, remote_db, isDone: multiprocessing.Value, internal_log
    ) -> None:
        try:
            DatabaseProcess.sync_worker(local_db, remote_db, isDone, internal_log)
        except Exception as e:
            with internal_log.error() as target:
                target.write(
                    "Unrecoverable exception in worker Process: {0} {1}\n".format(
                        type(e), str(e)
                    )
                )
                target.write("Stack trace: " + traceback.format_exc())
                target.write("\n")
            raise

    @staticmethod
    def sync_worker(
        local_db, remote_db, isDone: multiprocessing.Value, internal_log
    ) -> None:
        local_db.connect()
        remote_db.connect()

        while True:
            if isDone.value == 1:
                break
            sleep(5)
            try:
                local_db.ensure_connected()
                remote_db.ensure_connected()
                rows = local_db.fetch_logs()
                if rows:
                    for row in rows:
                        payload = dict(row)
                        payload.pop("id", None)

                        remote_db.insert_into(payload, "logs")
                        local_db.del_log(row["id"])
            except Exception as e:
                sleep_for = 10

                with internal_log.error() as target:
                    target.write(
                        "Could not sync database caused by: {0} {1}\n".format(
                            type(e), str(e)
                        )
                    )
                    if "open" in dir(remote_db.conn):
                        target.write(
                            "Remote DB handle state: {}\n".format(remote_db.conn.open)
                        )
                    target.write("Stack trace: " + traceback.format_exc())
                    target.write("\n\n")
                    target.write("Retry in {} s\n".format(sleep_for))

                sleep(sleep_for)

                with internal_log.error() as target:
                    target.write("Retrying now.\n")


class InternalLog:
    def __init__(self, save_path: str):
        self.save_path = save_path

    def debug(self):
        return self.open("debug")

    def error(self):
        return self.open("error")

    def open(self, level: str):
        target = open(os.path.join(self.save_path, level + ".log"), "a")
        line = "Log opened at: {} UTC\n".format(datetime.utcnow())
        target.write(line)
        target.write("=" * len(line) + "\n\n")
        return target


class Database:
    def __init__(self, path: str):
        self.conn = None
        self.path = path
        self.base_path = path
        self.internal_log = InternalLog(path)


class PlanetscaleDatabase(Database):
    def connect(self) -> None:
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=self.path + "/.env")
        self.conn = pymysql.connect(
            host=os.getenv("PS_HOST"),
            user=os.getenv("PS_USERNAME"),
            password=os.getenv("PS_PASSWORD"),
            db=os.getenv("PS_NAME"),
            autocommit=True,
            ssl={"mode": "True"},
        )

    def ensure_connected(self):
        if self.conn.open is False:
            self.connect()

    def insert_into(self, row, table="logs"):
        cols = ", ".join("`{}`".format(col) for col in row.keys())
        vals = ", ".join("%({})s".format(col) for col in row.keys())
        sql = "INSERT INTO `{}` ({}) VALUES ({})".format(table, cols, vals)
        self.conn.cursor().execute(sql, row)
        self.conn.commit()

    def fetch_from(self):
        sql = """
        SELECT
            `id`,
            `user`,
            `network`,
            `window`,
            `type`,
            `nick`,
            `message`
        FROM `inbound`
        """
        cur = self.conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(sql)
        return cur.fetchall()

    def del_from(self, iden):
        sql = "DELETE FROM inbound WHERE id = %s"
        self.conn.cursor().execute(sql, (iden,))
        self.conn.commit()


# Deprecated, use PlanetscaleDatabase instead
class MySQLDatabase(Database):
    def connect(self) -> None:
        self.internal_log.debug().write(
            "Using MySQL database is deprecated, please use Planetscale\n"
        )
        import pymysql
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=self.path + "/.env")
        self.conn = pymysql.connect(
            use_unicode=True,
            charset="utf8mb4",
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            host=os.getenv("MYSQL_HOST"),
            db=os.getenv("MYSQL_DATABASE"),
            port=int(os.getenv("MYSQL_PORT")),
            ssl={"mode": "True"},
        )

    def ensure_connected(self):
        if self.conn.open is False:
            self.connect()

    def insert_into(self, row, table="logs"):
        cols = ", ".join("`{}`".format(col) for col in row.keys())
        vals = ", ".join("%({})s".format(col) for col in row.keys())
        sql = "INSERT INTO `{}` ({}) VALUES ({})".format(table, cols, vals)
        self.conn.cursor().execute(sql, row)
        self.conn.commit()

    def fetch_from(self):
        sql = "SELECT * FROM inbound"
        cur = self.conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        self.conn.commit()
        return res

    def del_from(self, iden):
        sql = "DELETE FROM inbound WHERE id = %s"
        self.conn.cursor().execute(sql, (iden,))
        self.conn.commit()


class SQLiteDatabase(Database):
    def connect(self) -> None:
        import sqlite3

        # Ensure base directory exists
        os.makedirs(self.base_path, exist_ok=True)

        # Full path to buffer.sqlite
        db_path = os.path.join(self.base_path, "buffer.sqlite")

        # Connect (SQLite will create the file if it doesn't exist)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

        # Set pragmas for better performance
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")

        # Create tables if they don't exist
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT,
                user TEXT,
                network TEXT,
                window TEXT,
                type TEXT,
                nick TEXT,
                message TEXT
            )
            """
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inbound (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user TEXT,
                network TEXT,
                window TEXT,
                type TEXT,
                nick TEXT,
                message TEXT
            )
            """
        )

        self.conn.commit()

    def ensure_connected(self):
        if self.conn is None:
            self.connect()

    def insert_into(self, row, table="logs"):
        self.ensure_connected()
        cols = ", ".join(f"`{col}`" for col in row.keys())
        placeholders = ", ".join("?" for _ in row.keys())
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        values = [row[c] for c in row.keys()]
        self.conn.execute(sql, values)
        self.conn.commit()

    def fetch_from(self):
        self.ensure_connected()
        cur = self.conn.execute("SELECT id, * FROM inbound")
        return cur.fetchall()

    def del_from(self, iden):
        self.ensure_connected()
        self.conn.execute("DELETE FROM inbound WHERE id = ?", (iden,))
        self.conn.commit()

    def fetch_logs(self):
        self.ensure_connected()
        cur = self.conn.execute("SELECT id, * FROM logs")
        return cur.fetchall()

    def del_log(self, iden):
        self.ensure_connected()
        self.conn.execute("DELETE FROM logs WHERE id=?", (iden,))
        self.conn.commit()
