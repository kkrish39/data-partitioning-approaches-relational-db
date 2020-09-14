import psycopg2
from psycopg2 import sql
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
    interval = []
    splitValue = float(baseNumer)/float(numberOfParts)
    totalVal = 0

    for i in range(0, numberOfParts+1):
        interval.append(round(totalVal,2))
        totalVal += splitValue
    return interval

def findTableIndex(interval, ratingVal):
    for i in range(0, len(interval)-1):
        if((ratingVal > interval[i]) and ratingVal <= interval[i+1]):
            return i
    return 0

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    try:
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

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    try:
        if numberofpartitions > 0:
            cursor = openconnection.cursor()

            createMetaTable(cursor)
            insertMetaTable(cursor, RANGE_TABLE_PREFIX, numberofpartitions)

            interval = splitNumToIntervals(5, numberofpartitions)

            for i in range(0, numberofpartitions):
                createMoiveRatingsTable(RANGE_TABLE_PREFIX+str(i), cursor)
            
            for row in selectFromMovieRatingsTable(ratingstablename, cursor):
                tableIndex = findTableIndex(interval, row[2])
                insertIntoMovieRatingsTable(RANGE_TABLE_PREFIX+str(tableIndex),cursor,row[0],row[1],row[2])
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


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    try:
        cursor = openconnection.cursor()
        
        createMetaTable(cursor)
        insertMetaTable(cursor, RROBIN_TABLE_PREFIX, numberofpartitions)

        for i in range(0, numberofpartitions):
            createMoiveRatingsTable(RROBIN_TABLE_PREFIX+str(i), cursor)

        rows  = selectFromMovieRatingsTable(ratingstablename, cursor)

        for i in range(0, len(rows)):
            tableName = RROBIN_TABLE_PREFIX+str(i%numberofpartitions)
            insertIntoMovieRatingsTable(tableName, cursor, rows[i][0], rows[i][1], rows[i][2])
        
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


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        cursor = openconnection.cursor()
        
        count = rowCountOfMovieRatingsTable(ratingstablename, cursor)
        numPartitions = getNumPartition(cursor, RROBIN_TABLE_PREFIX)
        
        partitionTable = RROBIN_TABLE_PREFIX + str(count % int(numPartitions))

        insertIntoMovieRatingsTable(ratingstablename, cursor, userid, itemid,rating)
        insertIntoMovieRatingsTable(partitionTable,cursor,userid,itemid, rating)
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

def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        cursor = openconnection.cursor()
        numPartitions = getNumPartition(cursor, RANGE_TABLE_PREFIX)

        interval = splitNumToIntervals(5,numPartitions)
        
        tableIndex = findTableIndex(interval, rating)

        partitionTable = RANGE_TABLE_PREFIX + str(tableIndex)

        insertIntoMovieRatingsTable(ratingstablename,cursor,userid,itemid, rating)
        insertIntoMovieRatingsTable(partitionTable,cursor,userid,itemid, rating)
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

def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    try:
        cursor = openconnection.cursor()
        if(ratingMinValue < ratingMaxValue):
            numPartitionRangeTable = getNumPartition(cursor, RANGE_TABLE_PREFIX)
            numPartitionsRRobinTable = getNumPartition(cursor, RROBIN_TABLE_PREFIX)

            partitionArrayRangeTable = splitNumToIntervals(5,numPartitionRangeTable)

            constructedResult = []
            for i in range(0, numPartitionsRRobinTable):
                tableName = RROBIN_TABLE_PREFIX+str(i)
                returnedRows = selectFromMovieRatingsTableWithinRange(tableName, cursor, ratingMinValue, ratingMaxValue)
                for r in returnedRows:
                        constructedResult.append(tableName+","+str(r[0])+","+str(r[1])+","+str(r[2]))

            minRatingValueTableIndex = findTableIndex(partitionArrayRangeTable, ratingMinValue)
            maxRatingValueTableIndex = findTableIndex(partitionArrayRangeTable, ratingMaxValue)

            for i in range(minRatingValueTableIndex, maxRatingValueTableIndex+1):
                tableName = RANGE_TABLE_PREFIX+str(i)
                returnedRows = selectFromMovieRatingsTableWithinRange(tableName, cursor, ratingMinValue, ratingMaxValue)
                for r in returnedRows:
                    constructedResult.append(tableName+","+str(r[0])+","+str(r[1])+","+str(r[2]))
            
            writeRowsToFile(outputPath, constructedResult)
            openconnection.commit()
        elif ratingMinValue == ratingMaxValue:
            pointQuery(ratingMaxValue,openconnection,outputPath)
        else:
            print("Invalid Max and Min Rating range")
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

def pointQuery(ratingValue, openconnection, outputPath):
    try:
        cursor = openconnection.cursor()
        numPartitionRangeTable = getNumPartition(cursor, RANGE_TABLE_PREFIX)
        numPartitionsRRobinTable = getNumPartition(cursor, RROBIN_TABLE_PREFIX)

        partitionArrayRangeTable = splitNumToIntervals(5,numPartitionRangeTable)

        constructedResult = []
        
        for i in range(0, numPartitionsRRobinTable):
            tableName = RROBIN_TABLE_PREFIX+str(i)
            returnedRows = selectFromMovieRatingsTableWithPointRating(tableName, cursor, ratingValue)
            for r in returnedRows:
                constructedResult.append(tableName+","+str(r[0])+","+str(r[1])+","+str(r[2]))
        
        
        tableIndex = findTableIndex(partitionArrayRangeTable, ratingValue)

        tableName = RANGE_TABLE_PREFIX+str(tableIndex)
        returnedRows = selectFromMovieRatingsTableWithPointRating(tableName, cursor, ratingValue)

        for r in returnedRows:
            constructedResult.append(tableName+","+str(r[0])+","+str(r[1])+","+str(r[2]))
        
        writeRowsToFile(outputPath, constructedResult)
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
    
def writeRowsToFile(fileName, constructedResult):
    content = "\n".join(constructedResult)

    with open(fileName, "w") as file:
        file.write(content)

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
