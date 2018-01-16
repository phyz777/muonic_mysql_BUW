
from muonic.lib.consumers import AbstractMuonicConsumer
import mysql.connector as MySQL
from getpass import getpass

class MySqlConsumer(AbstractMuonicConsumer):
    """
    Writes data to a MySQL database
    """

    # insert something into runs
    # retrieve run ID
    # oh and retrieve user ID
    # there has to be a function that registers a user
    # shoot
    # do this later
    # for now
    # see the first two or three points

    def __init__(self, options):
        super(MySqlConsumer, self).__init__()
        self._conn = None
        self._cursor = None
        self.options = options
        self._connect()

        self.user_id = options.get('mysql_db_user_id', 0) # TODO fetch user_id if necessary

        self._decay_run_id = None
        self._pulse_run_id = None
        self._velocity_run_id = None
        self._rate_run_id = None

        self._init_tables()

    def _connect(self):
        # TODO: maybe add some output, this takes quite long;
        # TODO: maybe use the is_connected() method to test the connection
        pw = getpass("MySQL database password: ")

        self._conn = MySQL.connect(
            host=self.options.get("MySQL")[0],
            user=self.options.get("MySQL")[1],
            password=pw,
            database=self.options.get("MySQL")[2]
        )
        self._cursor = self._conn.cursor(buffered=True)

    def __del__(self):
        if self._conn is not None:
            self._conn.commit()
            self._conn.close()

    def start(self, run_id, analyzer_id='', expected_data_types=[]):
        pass

    def _register_decay_run(self):
        q1 = "INSERT INTO runs (`run_type`, `start_ts`, `stop_ts`, `sim`, `user_id`, `run_id`) "
        q1 += "VALUES ("
        q1 += "    'muon_decay', "
        q1 += "    NOW(), "
        q1 += "    NULL, "
        q1 += "    '" + str(int(self.options.get("sim"))) + "', "
        q1 += "    '" + str(self.user_id) + "', "
        q1 += "    NULL"
        q1 += ");"

        self._cursor.execute(q1)

        q2 = "SELECT LAST_INSERT_ID();"
        self._cursor.execute(q2)

        self._decay_run_id = self._cursor.fetchone()[0]

    def _register_pulse_run(self):
        q1 = "INSERT INTO runs (`run_type`, `start_ts`, `stop_ts`, `sim`, `user_id`, `run_id`) "
        q1 += "VALUES ("
        q1 += "    'pulse_analyzer', "
        q1 += "    NOW(), "
        q1 += "    NULL, "
        q1 += "    '" + str(int(self.options.get("sim"))) + "', "
        q1 += "    '" + str(self.user_id) + "', "
        q1 += "    NULL"
        q1 += ");"

        self._cursor.execute(q1)

        q2 = "SELECT LAST_INSERT_ID();"
        self._cursor.execute(q2)

        self._pulse_run_id = self._cursor.fetchone()[0]

    def _register_velocity_run(self):
        q1 = "INSERT INTO runs (`run_type`, `start_ts`, `stop_ts`, `sim`, `user_id`, `run_id`) "
        q1 += "VALUES ("
        q1 += "    'muon_velocity', "
        q1 += "    NOW(), "
        q1 += "    NULL, "
        q1 += "    '" + str(int(self.options.get("sim"))) + "', "
        q1 += "    '" + str(self.user_id) + "', "
        q1 += "    NULL"
        q1 += ");"

        self._cursor.execute(q1)

        q2 = "SELECT LAST_INSERT_ID();"
        self._cursor.execute(q2)

        self._velocity_run_id = self._cursor.fetchone()[0]

    def _register_rate_run(self):
        q1 = "INSERT INTO runs (`run_type`, `start_ts`, `stop_ts`, `sim`, `user_id`, `run_id`) "
        q1 += "VALUES ("
        q1 += "    'muon_rates', "
        q1 += "    NOW(), "
        q1 += "    NULL, "
        q1 += "    '" + str(int(self.options.get("sim"))) + "', "
        q1 += "    '" + str(self.user_id) + "', "
        q1 += "    NULL"
        q1 += ");"

        self._cursor.execute(q1)

        q2 = "SELECT LAST_INSERT_ID();"
        self._cursor.execute(q2)

        self._rate_run_id = self._cursor.fetchone()[0]

    def stop(self, run_id, analyzer_id=''):
        # print("DEBUG MySqlConsumer.stop START")

        if self._decay_run_id is not None:
            self.stop_decay()

        if self._pulse_run_id is not None:
            self.stop_pulse()

        if self._velocity_run_id is not None:
            self.stop_velocity()

        if self._rate_run_id is not None:
            self.stop_rate()

            # print("DEBUG MySqlConsumer.stop END")

    def stop_decay(self):
        q = "UPDATE runs SET `stop_ts`=NOW() WHERE `run_id`=" + str(self._decay_run_id) + ";"
        self._cursor.execute(q)
        self._conn.commit()

        self._decay_run_id = None

    def stop_pulse(self):
        q = "UPDATE runs SET `stop_ts`=NOW() WHERE `run_id`=" + str(self._pulse_run_id) + ";"
        self._cursor.execute(q)
        self._conn.commit()

        self._pulse_run_id = None

    def stop_velocity(self):
        q = "UPDATE runs SET `stop_ts`=NOW() WHERE `run_id`=" + str(self._velocity_run_id) + ";"
        self._cursor.execute(q)
        self._conn.commit()

        self._velocity_run_id = None

    def stop_rate(self):
        q = "UPDATE runs SET `stop_ts`=NOW() WHERE `run_id`=" + str(self._rate_run_id) + ";"
        self._cursor.execute(q)
        self._conn.commit()

        self._rate_run_id = None

    def push_decay(self, decay_time, event_time, meta):
        if self._decay_run_id is None:
            self._register_decay_run()

        q = "INSERT INTO muon_decay_data "
        q += "(`data_ts`, `decay_time`, `run_id`, `data_id`) "
        q += "VALUES ("
        q +=   "'" + str(event_time) + "', "
        q +=   str(decay_time) + ", "
        q +=   str(self._decay_run_id) + ", "
        q +=   "NULL"
        q += ");"

        self._cursor.execute(q)

    def push_pulse(self, pulse_widths, event_time, meta):
        if self._pulse_run_id is None:
            self._register_pulse_run()

        if len(pulse_widths[0]) + len(pulse_widths[1]) + len(pulse_widths[2]) + len(pulse_widths[3]) is 0:
            return

        q = "INSERT INTO pulse_analyzer_data "
        q += "(`data_ts`, `p_w_ch_0`, `p_w_ch_1`, `p_w_ch_2`, `p_w_ch_3`, `run_id`, `data_id`) "
        q += "VALUES "

        for i in range(len(pulse_widths[0])):
            q += "('"
            q += str(event_time) + "', "
            q += str(pulse_widths[0][i]) + ", "
            q += "NULL, NULL, NULL, "
            q += str(self._pulse_run_id) + ", "
            q += "NULL), "

        for i in range(len(pulse_widths[1])):
            q += "('"
            q += str(event_time) + "', "
            q += "NULL, "
            q += str(pulse_widths[1][i]) + ", "
            q += "NULL, NULL, "
            q += str(self._pulse_run_id) + ", "
            q += "NULL), "

        for i in range(len(pulse_widths[2])):
            q += "('"
            q += str(event_time) + "', "
            q += "NULL, NULL, "
            q += str(pulse_widths[2][i]) + ", "
            q += "NULL, "
            q += str(self._pulse_run_id) + ", "
            q += "NULL), "

        for i in range(len(pulse_widths[3])):
            q += "('"
            q += str(event_time) + "', "
            q += "NULL, NULL, NULL, "
            q += str(pulse_widths[3][i]) + ", "
            q += str(self._pulse_run_id) + ", "
            q += "NULL), "

        q = q[:-2] + ";"

        self._cursor.execute(q)

    def push_velocity(self, flight_time, event_time, meta):
        if self._velocity_run_id is None:
            self._register_velocity_run()

        q = "INSERT INTO muon_velocity_data "
        q += "(`data_ts`, `flight_time`, `run_id`, `data_id`) "
        q += "VALUES ("
        q +=   "'" + str(event_time) + "', "
        q +=   str(flight_time) + ", "
        q +=   str(self._velocity_run_id) + ", "
        q +=   "NULL"
        q += ");"

        self._cursor.execute(q)

    def push_raw(self, data, meta):
        pass

    def push_rate(self, rates, counts, time_window, query_time, meta):
        if self._rate_run_id is None:
            self._register_rate_run()

        q = "INSERT INTO muon_rates_data ("
        q += "  `data_ts`, `trig`, `delta`, "
        q += "  `rates_ch_0`, `rates_ch_1`, `rates_ch_2`, `rates_ch_3`, "
        q += "  `run_id`, `data_id`"
        q += ") VALUES ("
        q +=   "'" + str(query_time) + "', "
        q +=   str(rates[4]) + ", "
        q +=   str(time_window) + ", "
        q +=   str(rates[0]) + ", "
        q +=   str(rates[1]) + ", "
        q +=   str(rates[2]) + ", "
        q +=   str(rates[3]) + ", "
        q +=   str(self._rate_run_id) + ", "
        q +=   "NULL"
        q += ");"

        self._cursor.execute(q)

    def _init_tables(self):
        tableQueries = [
            "CREATE TABLE IF NOT EXISTS users("
            #  + "  name VARCHAR(30) NOT NULL DEFAULT \"Guest User\", "
            + "  name VARCHAR(30) NOT NULL, "
            + "  email VARCHAR(30) NOT NULL, "
            #  + "  signature CHAR(2) NOT NULL UNIQUE, "
            + "  user_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY"
            + ");",

            "CREATE TABLE IF NOT EXISTS runs("
            + "  run_type ENUM('muon_rates', 'pulse_analyzer', 'muon_decay', 'muon_velocity') NOT NULL, "
            + "  start_ts TIMESTAMP NOT NULL DEFAULT '1971-01-01 00:00:01', "
            + "  stop_ts TIMESTAMP NOT NULL DEFAULT '2037-01-19 03:14:07', "
            + "  sim BOOL NOT NULL DEFAULT FALSE, "
            + "  user_id SMALLINT UNSIGNED NOT NULL, "
            + "  FOREIGN KEY fk_user_id(user_id) REFERENCES users(user_id), "
            + "  run_id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY"
            + ");",

            "CREATE TABLE IF NOT EXISTS muon_rates_data("
            + "  data_ts TIMESTAMP(3) NOT NULL DEFAULT '1971-01-01 00:00:01.000', "
            + "  trig FLOAT NOT NULL, "
            + "  delta FLOAT NOT NULL, "
            + "  rates_ch_0 FLOAT NOT NULL, "
            + "  rates_ch_1 FLOAT NOT NULL, "
            + "  rates_ch_2 FLOAT NOT NULL, "
            + "  rates_ch_3 FLOAT NOT NULL, "
            + "  run_id MEDIUMINT UNSIGNED NOT NULL, "
            + "  FOREIGN KEY fk_run_id(run_id) REFERENCES runs(run_id), "
            + "  data_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY"
            + ");",

            "CREATE TABLE IF NOT EXISTS pulse_analyzer_data("
            + "  data_ts TIMESTAMP(6) NOT NULL DEFAULT '1971-01-01 00:00:01.000000', "
            + "  p_w_ch_0 FLOAT(2) NULL, "
            + "  p_w_ch_1 FLOAT(2) NULL, "
            + "  p_w_ch_2 FLOAT(2) NULL, "
            + "  p_w_ch_3 FLOAT(2) NULL, "
            + "  run_id MEDIUMINT UNSIGNED NOT NULL, "
            + "  FOREIGN KEY fk_run_id(run_id) REFERENCES runs(run_id), "
            + "  data_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY"
            + ");",

            "CREATE TABLE IF NOT EXISTS muon_decay_data("
            + "  data_ts TIMESTAMP(3) NOT NULL DEFAULT '1971-01-01 00:00:01.000', "
            + "  decay_time DOUBLE NOT NULL, "
            + "  run_id MEDIUMINT UNSIGNED NOT NULL, "
            + "  FOREIGN KEY fk_run_id(run_id) REFERENCES runs(run_id), "
            + "  data_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY"
            + ");",

            "CREATE TABLE IF NOT EXISTS muon_velocity_data("
            + "  data_ts TIMESTAMP(3) NOT NULL DEFAULT '1971-01-01 00:00:01.000', "
            + "  flight_time DOUBLE NOT NULL, "
            + "  run_id MEDIUMINT UNSIGNED NOT NULL, "
            + "  FOREIGN KEY fk_run_id(run_id) REFERENCES runs(run_id), "
            + "  data_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY"
            + ");"
        ]

        for qu in tableQueries:
            self._cursor.execute(qu)

        self._conn.commit()