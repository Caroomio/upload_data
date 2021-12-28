import csv
from datetime import datetime
import psycopg2
import json
from psycopg2.extras import Json
import sys


def upload_data(host, dbname, user, password, file_name):
    try:

        conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password}")

        cur = conn.cursor()

        print("Create partners_id_seq...")

        cur.execute("""
            CREATE SEQUENCE partners_id_seq START WITH 1;
        """)

        print("Done creating partners_id_seq")
        print("Creating Partners Table...")

        cur.execute("""
            CREATE TABLE "public"."partners" (
                "id" int8 NOT NULL DEFAULT nextval('partners_id_seq'::regclass),
                "name" varchar(255),
                "url" varchar(255),
                "address" varchar(255),
                "email" varchar(255),
                "phone" varchar(255),
                "county" varchar(255),
                "group_name" varchar(255),
                "partner_type" varchar(255),
                "info" jsonb,
                "inserted_at" timestamp NOT NULL,
                "updated_at" timestamp NOT NULL,
                "priority" int4 DEFAULT 5,
                "notify_email" varchar(255),
                "visible_in_claim" int4 DEFAULT 1,
                "visible_in_preclaim" int4 DEFAULT 0,
                "preclaim_notify_email" varchar(255),
                "ftp_details" jsonb,
                "location" varchar(255),
                "parent_id" int8,
                "callback_url" varchar(255),
                CONSTRAINT "partners_parent_id_fkey" FOREIGN KEY ("parent_id") REFERENCES "public"."partners"("id") ON DELETE SET NULL,
                PRIMARY KEY ("id")
            );
        """)

        print("Done Creating Partners Table.")

        """
        info, partner_type='GROUP', if partner_type then == 'DEALERSHIP' and enter group_name,
        """

        def transform_josn(data):
            data = json.loads(data)
            makes = data['makes']
            makes_ = {'listing_count': data['listing_count'], "carMake": {}}
            for make, value in makes.items():
                make_value = int(round(value / 100 * data['listing_count'], 0))
                makes_['carMake'].update({make.lower(): make_value})

            return makes_

        with open(file_name, 'r') as file:
            reader = csv.DictReader(file)

            next(reader)

            print("Filtering Data...")

            dealers_data = [
                {
                    "partner_type": "DEALERSHIP" if row['Dealership_Group_Name'] else "GROUP",
                    "group_name": row['Dealership_Group_Name'],
                    "info": Json(transform_josn(row['Dealer_Brand'])) if row.get('Dealer_Brand') else Json({})
                } for row in reader
            ]

            print("Dne filtering data")

            print("Inserting Data...")

            for data in dealers_data:
                data = list(data.values())
                data.append(datetime.now())
                data.append(datetime.now())
                cur.execute(
                    """
                    INSERT INTO partners (partner_type, group_name, info, inserted_at, updated_at) VALUES (%s, %s, %s, %s, %s);
                    """, data
                )


        conn.commit()

        print("Done Uploading")
    except Exception as e:
        print(str(e))


if __name__ == '__main__':
    host = sys.argv[1]
    dbname = sys.argv[2]
    user = sys.argv[3]
    password = sys.argv[4]
    file_name = sys.argv[5]

    upload_data(host, dbname, user, password, file_name)
