import PluginLoader
import datetime
import logging

# Postgres output customized for my specific monitoring setup
class PostgresOutput(PluginLoader.Plugin):
    def process_message(self, msg):
        import psycopg2

        try:
            with psycopg2.connect(self.config.get('postgresql', 'connstr')) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                    INSERT INTO production (system_id, date, production)
                    VALUES (1, current_date, %s)
                    ON CONFLICT (system_id, date) DO UPDATE SET production = excluded.production WHERE excluded.production > 0;
                    """, (msg.e_today * 1000,))

                    cur.execute("""
                    INSERT INTO omnik (
                        reading, kwh_total, kwh_today, inverter_temperature, inverter_hours,
                        pv_voltage, pv_current, ac_voltage, ac_current, ac_frequency, ac_power)
                    VALUES (now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (
                        msg.e_total,
                        msg.e_today,
                        msg.temperature,
                        msg.h_total,
                        msg.v_pv(1),
                        msg.i_pv(1),
                        msg.v_ac(1),
                        msg.i_ac(1),
                        msg.f_ac(1),
                        msg.p_ac(1)
                        ))
        except:
            logging.exception("Exception in PostgresOutput")
