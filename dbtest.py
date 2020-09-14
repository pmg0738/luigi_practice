import luigi
import time
import psycopg2


# https://datapipelinearchitect.com/luigi-query-postgresql/

class QueryPostgres(luigi.Task):
    def output(self):
        # the output will be a .csv file
        return luigi.LocalTarget("db_data/same_purchases.csv")

    def run(self):
        # these are here for convenience, you'll use
        # environment variables for production code
        host = "localhost"
        database = "luigi"
        user = "luigi"
        password = "luigi"

        conn = psycopg2.connect(
            dbname=database,
            user=user,
            host=host,
            password=password)
        cur = conn.cursor()
        cur.execute("""SELECT name FROM tasks INNER JOIN task_events ON tasks.id = task_events.task_id WHERE event_name = 'FAILED'""")
        rows = cur.fetchall()

        with self.output().open("w") as out_file:
            # write a csv header 'by hand'
            # out_file.write("id, task_id, event_name, ts")
            out_file.write("failed_taskname")

            for row in rows:
                out_file.write("\n")
                # without the :%s, the date will be output in year-month-day format
                # the star before row causes each element to be placed by itself into format
                # out_file.write("{}, {:%s}, {}, {}".format(*row))
                out_file.write("{}".format(*row))



if __name__ == '__main__':
    luigi.run(['QueryPostgres', '--workers', '1', '--local-scheduler'])
    # luigi.run(['tasks.Task2', '--workers', '1'])