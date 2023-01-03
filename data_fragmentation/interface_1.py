import psycopg2
import pandas as pd
import numpy as np

def get_ranges(n, s=0, e=5):
    """

    :param n:
    :param s:
    :param e:
    :return:
    """
    ranges = []
    step = (e - s) / n
    for i in range(n):
        ranges.append([i * step, i * step + step])
    return ranges

def Load_Ratings(path, conn):

    df = pd.read_csv(path, header=None, sep='::', engine='python', nrows=100)
    # data = [tuple(x) for x in df.iloc[:100, :3].to_numpy()]
    data = [tuple((str(int(x[0])), str(int(x[1])), str(x[2]))) for x in df.iloc[:, :3].to_numpy()]
    cursor = conn.cursor()
    # sql_string = "CREATE TABLE Ratings(UserID INT, " \
    #       "MovieID INT, " \
    #       "Rating  FLOAT)"

    # cursor.execute("DROP TABLE IF EXISTS Ratings")
    # cursor.execute(sql_string)
    # print('Table created successfully...')
    # conn.commit()
    sql_insert_query = "INSERT INTO Ratings (UserID, MovieID, Rating) VALUES (%s, %s, %s)"
    result = cursor.executemany(sql_insert_query, data)
    conn.commit()
    count = cursor.rowcount
    print(count, "Record inserted successfully into Ratings table")
    conn.close()

def Range_Partition(table, N, conn):
    ranges = get_ranges(N)
    cursor = conn.cursor()
    for i in range(N):
        table_name = table + "_range_part" + str(i)
        _range = ranges[i]
        s_r = _range[0] if i == 0 else _range[0] + 0.5
        e_r = _range[1] + 0.5
        print([s_r, e_r])
        query_str = "CREATE TABLE " + table_name + " PARTITION OF "+ table + " FOR VALUES FROM (" + str(s_r)+ ") TO (" + str(e_r)+ ")"
        cursor.execute(query_str)
        conn.commit()
    print("Partitions successfully created")
    conn.close()


def Range_Partition(table, N, conn):
    ranges = get_ranges(N)
    cursor = conn.cursor()
    for i in range(N):
        table_name = table + "_rrobin_part" + str(i)
        _range = ranges[i]
        s_r = _range[0] if i == 0 else _range[0] + 0.5
        e_r = _range[1] + 0.5
        print([s_r, e_r])
        query_str = "CREATE TABLE " + table_name + " PARTITION OF "+ table + " FOR VALUES FROM (" + str(s_r)+ ") TO (" + str(e_r)+ ")"
        cursor.execute(query_str)
        conn.commit()
    print("Partitions successfully created")
    conn.close()

def Range_Insert(table, userID, movieID, rating, conn):
    cursor = conn.cursor()
    query_str = "INSERT INTO " + table + "(UserID, MovieID, Rating) VALUES (%s, %s, %s)"
    cursor.execute(query_str, (userID, movieID, rating))
    conn.commit()
    print("successfully done!")
    conn.close()


if __name__ == "__main__":
   conn = psycopg2.connect(database="movie recommendation database", user="postgres", password="fkh49863018",
                            host="localhost")

   # path = '/Users/fatemeh/Documents/Python_Projects/ASU/assignment3/ratings.dat'
   # Load_Ratings(path,conn)
   # Range_Partition('ratings', 2, conn)
   Range_Insert('ratings', 1, 122, 2.5, conn)




