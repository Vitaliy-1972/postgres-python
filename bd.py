import psycopg2
from psycopg2.sql import SQL, Identifier

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS person(
            person_id SERIAL PRIMARY KEY,
            first_name VARCHAR(64) NOT NULL,
            second_name VARCHAR(64) NOT NULL,
            email VARCHAR (80) UNIQUE NOT NULL,
            phones BOOLEAN
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone(
            phone_id SERIAL PRIMARY KEY,
            person_id integer NOT NULL,
            foreign key (person_id) references person (person_id),
            phone_number VARCHAR (64) NULL
        );
        """)
        conn.commit()


def add_person(conn, first_name, second_name, email, phones=False):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO person(first_name, second_name, email, phones)
        VALUES (%s, %s, %s,%s)
        """, (first_name, second_name, email, phones))
        conn.commit()


def add_phone(conn, person_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO phone( person_id, phone_number)
        VALUES (%s, %s);
        """, (person_id, phone_number))
        conn.commit()


def chande_person(conn, person_id, first_name=None, second_name=None, email=None):
    with conn.cursor() as cur:
        arg_dict = {'first_name': first_name, 'second_name': second_name, 'email':email,}
        for key, arg in arg_dict.items():
            if arg:
                cur.execute(SQL("""
                UPDATE person
                SET {} = %s
                WHERE person_id = %s;""").format(Identifier(key)), (arg, person_id))
                conn.commit()

def delete_phone(conn, person_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone 
        WHERE person_id=%s AND phone_number=%s;
        """, (person_id, phone_number))
        conn.commit()


def delete_person(conn, person_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone 
        WHERE person_id=%s;
        """, (person_id,))
        cur.execute("""
        DELETE FROM person 
        WHERE person_id=%s;
        """, (person_id,))
        conn.commit()


def find_person(conn, first_name=None, second_name=None, email=None, phone_number=None):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM person p
        LEFT JOIN phone ph ON p.person_id = ph.person_id
        WHERE (first_name=%(first_name)s OR %(first_name)s IS NULL)
        AND (second_name=%(second_name)s OR %(second_name)s IS NULL)
        AND (email=%(email)s OR %(email)s IS NULL)
        AND (phone_number=%(phone_number)s OR %(phone_number)s IS NULL);
        """, {'first_name': first_name, 'second_name': second_name,
              'email':email,'phone_number':phone_number})
        return cur.fetchone()

if __name__ == "__main__":
    with psycopg2.connect(database='python_db', user='postgres', password='af49vo') as conn:

        # create_table(conn)
        # add_person(conn, 'ivan', 'ivanov', 'ivanov@yandex.ru', phones=True)
        # add_person(conn, 'ivan', 'sidorov', 'sidorov@yandex.ru', phones=True)
        # add_phone(conn, 1, '21')
        # add_phone(conn, 1, '22')
        # add_phone(conn, 2, '23')
        # chande_person(conn, 2,'petr')
        # delete_phone(conn, 1, '21')
        # delete_person(conn, 1)
        print(find_person(conn, 'petr'))
