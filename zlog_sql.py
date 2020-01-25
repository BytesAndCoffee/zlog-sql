import inspect
import json
import multiprocessing
import os
import pprint
import re
import traceback
import warnings
from datetime import datetime
from time import sleep
import znc


class zlog_sql(znc.Module):
    description = 'Logs all channels to a MySQL database.'
    module_types = [znc.CModInfo.GlobalModule]

    wiki_page = 'ZLog_SQL'

    has_args = True
    args_help_text = 'Connection string in format: mysql://user:pass@host/database_name'

    log_queue = multiprocessing.SimpleQueue()
    reply_queue = multiprocessing.SimpleQueue()
    internal_log = None
    hook_debugging = False



    def OnLoad(self, args, message):
        """
        This module hook is called when a module is loaded.
        :type args: const CString &
        :type args: CString &
        :rtype: bool
        :param args: The arguments for the modules.
        :param message: A message that may be displayed to the user after loading the module.
        :return: True if the module loaded successfully, else False.
        """
        self.lookup = dict()
        self.types = {'msg': 'PRIVMSG', 'action': 'ACTION'}
        self.internal_log = InternalLog(self.GetSavePath())
        self.debug_hook()

        try:
            db = self.parse_args(args)
            multiprocessing.Process(target=DatabaseThread.worker_safe,
                                    args=(
                                        db,
                                        self.log_queue,
                                        self.reply_queue,
                                        self.internal_log
                                    )).start()
            return True
        except Exception as e:
            message.s = str(e)

            with self.internal_log.error() as target:
                target.write('Could not initialize module caused by: {} {}\n'.format(type(e), str(e)))
                target.write('Stack trace: ' + traceback.format_exc())
                target.write('\n')

            return False

    def OnShutdown(self):
        # Terminate worker process.
        self.log_queue.put(None)

    def GetServer(self):
        pServer = self.GetNetwork().GetCurrentServer()

        if pServer is None:
            return '(no server)'

        sSSL = '+' if pServer.IsSSL() else ''
        return pServer.GetName() + ' ' + sSSL + pServer.GetPort()

    # GENERAL IRC EVENTS
    # ==================

    def OnIRCConnected(self):
        """
        This module hook is called after a successful login to IRC.
        :rtype: None
        """
        self.debug_hook()
        self.put_log('connect', self.GetServer())

    def OnIRCDisconnected(self):
        """
        This module hook is called when a user gets disconnected from IRC.
        :rtype: None
        """
        self.debug_hook()
        self.put_log('disconnect', self.GetServer())

    def OnBroadcast(self, message):
        """
        This module hook is called when a message is broadcasted to all users.
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log(
            'broadcast',
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
        sNick = opNick.GetNick() if opNick is not None else 'Server'
        self.put_log(
            'rawmode',
            sNick + ' sets mode: ' + modes + ' ' + args,
            channel.GetName(),
            sNick
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
            'kick',
            kickedNick + ' was kicked by ' + opNick.GetNick() + ' (' + message + ')',
            channel.GetName(),
            kickedNick
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
                'quit',
                nick.GetNick() + ' (' + nick.GetIdent() + '@' + nick.GetHost() + ') (' + message + ')',
                channel.GetName(),
                nick.GetNick()
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
            'join',
            nick.GetNick() + ' (' + nick.GetIdent() + '@' + nick.GetHost() + ')',
            channel.GetName(),
            nick.GetNick()
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
            'part',
            nick.GetNick() + ' (' + nick.GetIdent() + '@' + nick.GetHost() + ') (' + message + ')',
            channel.GetName(),
            nick.GetNick()
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
                'nick',
                oldNick.GetNick() + ' is now known as ' + newNick,
                channel.GetName(),
                oldNick.GetNick()
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
        self.put_log(
            'topic',
            str(topic),
            channel.GetName(),
            nick.GetNick()
        )
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
            self.put_log(
                'notice',
                str(message),
                str(target),
                network.GetCurNick()
            )
        return znc.CONTINUE

    def OnPrivNotice(self, nick, message):
        """
        Called when we receive a private NOTICE message from IRC.
        :type nick: CNick &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log(
            'notice',
            str(message),
            nick.GetNick(),
            nick.GetNick()
        )
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
        self.put_log(
            'notice',
            str(message),
            channel.GetName(),
            nick.GetNick()
        )
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
            self.put_log(
                'action',
                str(message),
                str(target),
                pNetwork.GetCurNick()
            )
        return znc.CONTINUE

    def OnPrivAction(self, nick, message):
        """
        Called when we receive a private CTCP ACTION ("/me" in query) from IRC.
        :type nick: CNick &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log(
            'action',
            str(message),
            nick.GetNick(),
            nick.GetNick()
        )
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
        self.put_log(
            'action',
            str(message),
            channel.GetName(),
            nick.GetNick()
        )
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
            self.put_log(
                'msg',
                str(message),
                str(target),
                network.GetCurNick()
            )
        return znc.CONTINUE

    def OnPrivMsg(self, nick, message):
        """
        Called when we receive a private PRIVMSG message from IRC.
        :type nick: CNick &
        :type message: CString &
        :rtype: EModRet
        """
        self.debug_hook()
        self.put_log(
            'msg',
            str(message),
            nick.GetNick(),
            nick.GetNick()
        )
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
        self.put_log(
            'msg',
            str(message),
            channel.GetName(),
            nick.GetNick()
        )
        return znc.CONTINUE

    # LOGGING
    # =======

    def put_irc(self, user, network, window, mtype, target, message):
        with self.internal_log.error() as log:
            query = None
            log.write('Writing to IRC...\n')
            if self.lookup[user, network]:
                log.write('Looking up object for {}\n'.format(str((user, network))))
                query = self.lookup[(user, network)]
                log.write('Found! {}\n'.format(str(query)))
            else:
                log.write('Locating user {} and network {}\n'.format(user, network))
                query = znc.CZNC.Get().FindUser(user)
                if query is not None:
                    log.write('User {} found, locating network {}\n'.format(user, network))
                    query = query.FindNetwork(network)
                    if query is not None:
                        log.write('Lookup complete! located user {} and network {}. Storing in cache\n'.format(user, network))
                        self.lookup[user, network] = query
                    else:
                        log.write('Lookup failed, user {} found but network {} not found\n'.format(user, network))
                else:
                    log.write('Lookup failed, user {} not found\n'.format(user))
            if query is not None:
                log.write('Sending line...\n')
                if window == target:
                    line = '{} {} :{}'.format(self.types[mtype], window, message)
                else:
                    line = '{} {} :{}: {}'.format(self.types[mtype], window, target, message)
                log.write(str(query) + '\n')
                log.write('[{}]\n'.format(line))
                query.PutIRC(line)

    def put_log(self, mtype, line, window="Status", nick=None):
        """
        Adds the log line to database write queue.
        """
        self.log_queue.put({
            'created_at': datetime.utcnow().isoformat(),
            'user': self.GetUser().GetUserName() if self.GetUser() is not None else None,
            'network': self.GetNetwork().GetName() if self.GetUser() is not None else None,
            'window': window,
            'type': mtype,
            'nick': nick,
            'message': line.encode('utf8', 'replace').decode('utf8')})
        if not self.reply_queue.empty():
            line = self.reply_queue.get()
            with self.internal_log.error() as target:
                target.write('* {} *\n'.format(list(line)))
            self.put_irc(line[1], line[2], line[3], line[4], line[5], line[6])


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
            target.write('Called method: ' + frameinfo.function + '()\n')
            for argname in argvals:
                if argname == 'self':
                    continue
                target.write('    ' + argname + ' -> ' + pprint.pformat(argvals[argname]) + '\n')
            target.write('\n')

    # ARGUMENT PARSING
    # ================

    def parse_args(self, args):
        if args.strip() == '':
            raise Exception('Missing argument. Provide connection string as an argument.')

        match = re.search('^\s*mysql://(.+?):(.+?)@(.+?)/(.+)\s*$', args)
        if match:
            return MySQLDatabase({'host': match.group(3),
                                  'user': match.group(1),
                                  'passwd': match.group(2),
                                  'db': match.group(4)})

        raise Exception('Unrecognized connection string. Check the documentation.')


class DatabaseThread:
    @staticmethod
    def worker_safe(
            db,
            log_queue: multiprocessing.SimpleQueue,
            inbound_queue: multiprocessing.SimpleQueue,
            internal_log
    ) -> None:
        try:
            DatabaseThread.worker(db, log_queue, inbound_queue, internal_log)
        except Exception as e:
            with internal_log.error() as target:
                target.write('Unrecoverable exception in worker thread: {0} {1}\n'.format(type(e), str(e)))
                target.write('Stack trace: ' + traceback.format_exc())
                target.write('\n')
            raise

    @staticmethod
    def worker(
            db,
            log_queue: multiprocessing.SimpleQueue,
            inbound_queue: multiprocessing.SimpleQueue,
            internal_log
    ) -> None:
        db.connect()

        while True:
            item = log_queue.get()
            if item is None:
                break

            try:
                db.ensure_connected()
                db.insert_into(item, 'logs')
                res = db.fetch_from()
                with internal_log.error() as target:
                    target.write('== ' + str(res) + ' ==\n')
                    if res:
                        for line in res:
                            target.write(str(line) + '\n')
                            inbound_queue.put(line)
                            db.del_from(line[0])
            except Exception as e:
                sleep_for = 10

                with internal_log.error() as target:
                    target.write('Could not save to database caused by: {0} {1}\n'.format(type(e), str(e)))
                    if 'open' in dir(db.conn):
                        target.write('Database handle state: {}\n'.format(db.conn.open))
                    target.write('Stack trace: ' + traceback.format_exc())
                    target.write('Current log: ')
                    json.dump(item, target)
                    target.write('\n\n')
                    target.write('Retry in {} s\n'.format(sleep_for))

                sleep(sleep_for)

                with internal_log.error() as target:
                    target.write('Retrying now.\n'.format(sleep_for))
                    log_queue.put(item)


class InternalLog:
    def __init__(self, save_path: str):
        self.save_path = save_path

    def debug(self):
        return self.open('debug')

    def error(self):
        return self.open('error')

    def open(self, level: str):
        target = open(os.path.join(self.save_path, level + '.log'), 'a')
        line = 'Log opened at: {} UTC\n'.format(datetime.utcnow())
        target.write(line)
        target.write('=' * len(line) + '\n\n')
        return target


class Database:
    def __init__(self, dsn: dict):
        self.dsn = dsn
        self.conn = None


class MySQLDatabase(Database):
    def connect(self) -> None:
        import pymysql
        self.conn = pymysql.connect(use_unicode=True, charset='utf8mb4', **self.dsn)

    def ensure_connected(self):
        if self.conn.open is False:
            self.connect()

    def insert_into(self, row, table="logs"):
        cols = ', '.join('`{}`'.format(col) for col in row.keys())
        vals = ', '.join('%({})s'.format(col) for col in row.keys())
        sql = 'INSERT INTO `{}` ({}) VALUES ({})'.format(table, cols, vals)
        self.conn.cursor().execute(sql, row)
        self.conn.commit()

    def fetch_from(self):
        sql = 'SELECT * FROM inbound'
        cur = self.conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        return res

    def del_from(self, iden):
        sql = 'DELETE FROM inbound WHERE id = {}'.format(iden)
        self.conn.cursor().execute(sql)
        self.conn.commit()