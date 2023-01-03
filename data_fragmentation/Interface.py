#!/usr/bin/python3
#
# Interface for the assignement
#

import psycopg2
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
META_TABLE = 'metatable'
min_rate = 0.0
max_rate = 5.0

import csv
def getOpenConnection(user='postgres', password='fkh49863018', dbname='movie recommendation database'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
    openconnection.commit()
    cur.execute(
        "CREATE TABLE " + ratingstablename + " (UserID INT, MovieID INT, Rating Float)")
    openconnection.commit()

    # df = pd.read_csv(ratingsfilepath, header=None, sep='::', engine='python',)
    # data = [tuple((int(x[0]), int(x[1]), x[2])) for x in df.iloc[:, :3].to_numpy()]
    loadout = open(ratingsfilepath, 'r')
    data = []
    for r in loadout.readlines():
        s = r.index("::")
        a_1 = r[:s]
        r = r[s+2:]
        s = r.index("::")
        a_2 = r[:s]
        r = r[s+2:]
        s = r.index("::")
        a_3 = r[:s]
        r = r[s+2:]
        data.append((a_1, a_2, a_3))

    sql_insert_query = "INSERT INTO Ratings (UserID, MovieID, Rating) VALUES (%s, %s, %s)"
    cur.executemany(sql_insert_query, data)
    openconnection.commit()

    cur.close()

def get_ranges(n, s=0, e=5):
    ranges = []
    step = (e - s) / n
    for i in range(n):
        ranges.append([i * step, i * step + step])
    return ranges

def rangePartition(ratingstablename, numberofpartitions, openconnection):

    cursor = openconnection.cursor()
    # create meta table
    cursor.execute("CREATE TABLE IF NOT EXISTS %s(partition_number int)" % (META_TABLE))
    query = "INSERT INTO " + META_TABLE + "(partition_number) VALUES(%s)"
    cursor.execute(query, str(numberofpartitions))
    openconnection.commit()

    cursor.execute("SELECT * FROM %s" % (ratingstablename))
    table_rows = cursor.fetchall()


    for i in range(numberofpartitions):
        t_name = RANGE_TABLE_PREFIX + str(i)
        cursor.execute("CREATE TABLE IF NOT EXISTS %s(UserID int, MovieID int, Rating Float)" % (t_name))
    openconnection.commit()
    ranges = get_ranges(numberofpartitions, min_rate, max_rate)
    values = []
    # ***it can be solved by using select from the mian table based the ranges and insert into the partitions***
    for r_i in range(numberofpartitions):
        t_name = RANGE_TABLE_PREFIX + str(r_i)
        for row in table_rows:
            if r_i == 0:
                if row[2] >= ranges[r_i][0] and row[2] <= ranges[r_i][1]:
                  values.append(row)
            else:
                if row[2] > ranges[r_i][0] and row[2] <= ranges[r_i][1]:
                  values.append(row)

        query = "INSERT INTO " + t_name + "(UserID, MovieID, Rating) VALUES(%s, %s, %s)"
        cursor.executemany(query, values)
        values = []
    openconnection.commit()
    cursor.close()

def get_rr_idx(n_row, n_part):
    idxes = []
    for i in range(n_part):
        idxes.append([i for i in range(i, n_row, n_part)])
        # idxes.append(np.arange(i, n_row, n_part))
    return idxes

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()
    # create meta table
    cursor.execute("CREATE TABLE IF NOT EXISTS %s(partition_number int, number_rows int)" % (META_TABLE))

    cursor.execute("SELECT * FROM %s" % (ratingstablename))
    # table_rows = np.asarray(cursor.fetchall(), dtype=object)
    table_rows = cursor.fetchall()
    # out = np.empty(len(table_rows), dtype=object)
    # out[:] = table_rows

    for i in range(numberofpartitions):
        t_name = RROBIN_TABLE_PREFIX + str(i)
        cursor.execute("CREATE TABLE IF NOT EXISTS %s(UserID int, MovieID int, Rating Float)" % (t_name))
    openconnection.commit()
    idxes = get_rr_idx(len(table_rows), numberofpartitions)
    for r_i in range(numberofpartitions):
        t_name = RROBIN_TABLE_PREFIX + str(r_i)
        values = []
        for id in idxes[r_i]:
            values.append(table_rows[id])
        # values = table_rows[idxes[r_i]]
        query = "INSERT INTO " + t_name + "(UserID, MovieID, Rating) VALUES(%s, %s, %s)"
        cursor.executemany(query, values)
    openconnection.commit()

    query = "INSERT INTO " + META_TABLE + "(partition_number, number_rows) VALUES(%s, %s)"
    row = (numberofpartitions, len(table_rows))
    cursor.execute(query, row)
    openconnection.commit()
    cursor.close()



def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    cursor.execute("SELECT * FROM %s" % (META_TABLE))
    row = cursor.fetchone()
    numberofpartitionsm, num_rows = row[0], row[1]
    t_name = RROBIN_TABLE_PREFIX + str(num_rows%numberofpartitionsm)
    query_str = "INSERT INTO " + t_name + "(UserID, MovieID, Rating) VALUES(%s, %s, %s)"
    cursor.execute(query_str, (userid, itemid, rating))
    openconnection.commit()
    cursor.close()


def get_partition_name(numberofpartitions, rating):
    ranges = get_ranges(numberofpartitions, min_rate, max_rate)
    for i in range(numberofpartitions):
        if i == 0:
            if rating >= ranges[i][0] and rating <= ranges[i][1]:
                break
        else:
            if rating > ranges[i][0] and rating <= ranges[i][1]:
                break
    return RANGE_TABLE_PREFIX + str(i)

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    cursor.execute("SELECT partition_number FROM %s" % (META_TABLE))
    numberofpartitions = cursor.fetchone()
    numberofpartitions = numberofpartitions[0]

    partition_tablename = get_partition_name(numberofpartitions, rating)

    query_str = "INSERT INTO " + partition_tablename + "(UserID, MovieID, Rating) VALUES (%s, %s, %s)"
    cursor.execute(query_str, (userid, itemid, rating))
    openconnection.commit()
    cursor.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()

