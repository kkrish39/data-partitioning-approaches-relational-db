import psycopg2
import numpy as np
from psycopg2 import sql
import bisect
import os
import sys

RANGE_TABLE_PREFIX = 'range_ratings_part'
RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'
META_TABLE = 'fragment_meta'

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def createMoiveRatingsTable(tableName, cursor):
    cursor.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (userid int, movieid int, rating float)").format(sql.Identifier(tableName)))

def selectFromMovieRatingsTable(tableName, cursor):
    cursor.execute("SELECT * from {}".format(tableName))
    return cursor.fetchall()

def selectFromMovieRatingsTableWithinRange(tableName, cursor, lowR, highR):
    cursor.execute("SELECT * from {} where rating >= %s and rating <= %s".format(tableName),[lowR, highR])
    return cursor.fetchall()

def selectFromMovieRatingsTableWithPointRating(tableName, cursor, rating):
    cursor.execute("SELECT * from {} where rating = %s".format(tableName),[rating])
    return cursor.fetchall()

def insertIntoMovieRatingsTable(tableName, cursor, userid, itemid, rating):
    cursor.execute(sql.SQL("INSERT INTO {} (userid, movieid, rating) VALUES(%s,%s,%s)").format(sql.Identifier(tableName)), [userid, itemid, rating])

def rowCountOfMovieRatingsTable(tableName, cursor):
    cursor.execute(sql.SQL("SELECT COUNT(*) from {}").format(sql.Identifier(tableName)))
    return cursor.fetchone()[0]

def createMetaTable(cursor):
    cursor.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (partition_id varchar(50) PRIMARY KEY, num_partitions int)").format(sql.Identifier(META_TABLE)))

def insertMetaTable(cursor, partitionIdentifier, numPartitions):
    cursor.execute(sql.SQL("INSERT INTO {} (partition_id, num_partitions) VALUES(%s, %s)").format(sql.Identifier(META_TABLE)), [partitionIdentifier, numPartitions])

def getNumPartition(cursor, partitionIdentifier):
    cursor.execute(sql.SQL("SELECT num_partitions from {} where partition_id=%s").format(sql.Identifier(META_TABLE)), [partitionIdentifier])
    return cursor.fetchone()[0]

def splitNumToIntervals(baseNumer, numberOfParts):
    return np.round(np.linspace(0, baseNumer, numberOfParts+1),2)

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    # Getting the cursor pointer from the open connection.
    cursor = openconnection.cursor()

    # Create table for the given ratingstablename
    createMoiveRatingsTable(ratingstablename, cursor)

    # read file data from the given path
    fileData = open(ratingsfilepath)
    for line in fileData:
        # split each line of the file having :: separator
        rowToBeInserted = line.split("::")
        # Insert userid, movieid, rating to the table
        insertIntoMovieRatingsTable(ratingstablename, cursor, rowToBeInserted[0], rowToBeInserted[1], rowToBeInserted[2])


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()

    createMetaTable(cursor)
    insertMetaTable(cursor, RANGE_TABLE_PREFIX, numberofpartitions)

    interval = splitNumToIntervals(5, numberofpartitions)
    interval = interval.astype(float)
    for i in range(0, numberofpartitions):
        createMoiveRatingsTable(RANGE_TABLE_PREFIX+str(i), cursor)
    
    for row in selectFromMovieRatingsTable(ratingstablename, cursor):
        tableIndex = bisect.bisect_left(interval, row[2]) - 1
        tableIndex = 0 if tableIndex < 0 else tableIndex
        insertIntoMovieRatingsTable(RANGE_TABLE_PREFIX+str(tableIndex),cursor,row[0],row[1],row[2])


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()
    
    createMetaTable(cursor)
    insertMetaTable(cursor, RROBIN_TABLE_PREFIX, numberofpartitions)

    for i in range(0, numberofpartitions):
        createMoiveRatingsTable(RROBIN_TABLE_PREFIX+str(i), cursor)

    rows  = selectFromMovieRatingsTable(ratingstablename, cursor)

    for i in range(0, len(rows)):
        tableName = RROBIN_TABLE_PREFIX+str(i%numberofpartitions)
        insertIntoMovieRatingsTable(tableName, cursor, rows[i][0], rows[i][1], rows[i][2])


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    
    count = rowCountOfMovieRatingsTable(ratingstablename, cursor)
    numPartitions = getNumPartition(cursor, RROBIN_TABLE_PREFIX)
    
    partitionTable = RROBIN_TABLE_PREFIX + str(count % int(numPartitions))

    insertIntoMovieRatingsTable(ratingstablename, cursor, userid, itemid,rating)
    insertIntoMovieRatingsTable(partitionTable,cursor,userid,itemid, rating)


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    numPartitions = getNumPartition(cursor, RANGE_TABLE_PREFIX)

    partitionArray = splitNumToIntervals(5,numPartitions)
    
    for i in range(1, numPartitions+1):
        if(rating <= partitionArray[i]):
            partitionTable = RANGE_TABLE_PREFIX + str(i-1)
            break

    insertIntoMovieRatingsTable(ratingstablename,cursor,userid,itemid, rating)
    insertIntoMovieRatingsTable(partitionTable,cursor,userid,itemid, rating)


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    cursor = openconnection.cursor()

    # numPartitionRangeTable = getNumPartition(cursor, RANGE_TABLE_PREFIX)
    # numPartitionsRRobinTable = getNumPartition(cursor, RROBIN_TABLE_PREFIX)

    # partitionArrayRangeTable = splitNumToIntervals(5,numPartitionRangeTable)

    # constructedResult = []

    # for i in range(0, numPartitionsRRobinTable):
    #     tableName = RROBIN_TABLE_PREFIX+str(i)
    #     returnedRows = selectFromMovieRatingsTableWithinRange(tableName, cursor, ratingMinValue, ratingMaxValue)
    #     for r in returnedRows:
    #             constructedResult.append(tableName+","+str(r[0])+","+str(r[1])+","+str(r[2]))
    
    # ratingMinValue = 3.5
    # ratingMaxValue = 4.5
    # print(ratingMinValue, ratingMaxValue)
    # for i in range(0, numPartitionRangeTable):
    #     if  partitionArrayRangeTable[i] <= ratingMinValue and partitionArrayRangeTable[i+1] >= ratingMinValue: 
    #         tableName = RANGE_TABLE_PREFIX+str(i)
    #         print("----------------",partitionArrayRangeTable[i])
    #         i = i + 1
    #         while partitionArrayRangeTable[i] >= ratingMaxValue and partitionArrayRangeTable[i+1] <= ratingMaxValue and i < numPartitionRangeTable:
    #             tableName = RANGE_TABLE_PREFIX+str(i)
    #             print("----------------",tableName)
    #             i = i+1
    #         break
    #             # returnedRows = selectFromMovieRatingsTableWithinRange(tableName, cursor, partitionArrayRangeTable[i], partitionArrayRangeTable[i+1])
    #             # for r in returnedRows:
    #             #     constructedResult.append(tableName+","+str(r[0])+","+str(r[1])+","+str(r[2]))

    # writeRowsToFile(outputPath, constructedResult)

def pointQuery(ratingValue, openconnection, outputPath):
    cursor = openconnection.cursor()
    
    numPartitionRangeTable = getNumPartition(cursor, RANGE_TABLE_PREFIX)
    numPartitionsRRobinTable = getNumPartition(cursor, RROBIN_TABLE_PREFIX)

    partitionArrayRangeTable = splitNumToIntervals(5,numPartitionRangeTable)

    constructedResult = []
    for i in range(1, numPartitionRangeTable+1):
        if ratingValue <= partitionArrayRangeTable[i]:
            tableName = RANGE_TABLE_PREFIX+str(i-1)
            returnedRows = selectFromMovieRatingsTableWithPointRating(tableName, cursor, ratingValue)
            for r in returnedRows:
                constructedResult.append(tableName+","+str(r[0])+","+str(r[1])+","+str(r[2]))
            break
    
    
    for i in range(0, numPartitionsRRobinTable):
        tableName = RROBIN_TABLE_PREFIX+str(i)
        returnedRows = selectFromMovieRatingsTableWithPointRating(tableName, cursor, ratingValue)
        for r in returnedRows:
                constructedResult.append(tableName+","+str(r[0])+","+str(r[1])+","+str(r[2]))
    
    writeRowsToFile(outputPath, constructedResult)
    

def writeRowsToFile(fileName, constructedResult):
    with open(fileName, "w") as file:
        for line in constructedResult:
            file.writelines(line + "\n")

def createDB(dbname='dds_assignment1'):
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
