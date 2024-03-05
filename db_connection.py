#-------------------------------------------------------------------------
# AUTHOR: Martin Lado
# FILENAME: db_connection.py
# SPECIFICATION: description of the program
# FOR: CS 4250 - Assignment #2
# TIME SPENT: 3 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays


import psycopg2
from psycopg2.extras import RealDictCursor

def connectDataBase():
    DB_NAME = "4250-HW2"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        print("Database connected successfully")
        createTables(conn.cursor(), conn)
        return conn

    except:
        print("Database could not be connected")
        
def createTables(cur, conn):
    try:
        sql = "CREATE TABLE categories (id_cat text,name text, CONSTRAINT category_pk PRIMARY KEY (id_cat))"
        cur.execute(sql)
        sql = "CREATE TABLE documents (doc text, text text," \
              "title text, num_chars text, date text, id_cat text," \
              "CONSTRAINT documents_pk PRIMARY KEY (doc), CONSTRAINT category_fk FOREIGN KEY (id_cat) " \
              "REFERENCES categories (id_cat))"
        cur.execute(sql)
        sql = "CREATE TABLE terms (term text, num_chars text," \
              "CONSTRAINT terms_pk PRIMARY KEY (term))"
        cur.execute(sql)
        sql = "CREATE TABLE index (doc text, term text, term_count text," \
              "CONSTRAINT index_pk PRIMARY KEY (doc, term), " \
              "CONSTRAINT document_index_fk FOREIGN KEY (doc) REFERENCES documents (doc)," \
              "CONSTRAINT term_index_fk FOREIGN KEY (term) REFERENCES terms (term))"
        cur.execute(sql)
        conn.commit()
    except:
        conn.rollback()
        print ("Could not create databse or database may already exist.")


def createCategory(cur, catId, catName):
    sql = "Insert into categories (id_cat, name) Values (%s, %s)"
    recset = [catId, catName]
    cur.execute(sql, recset)

def createDocument(cur, docId, docText, docTitle, docDate, docCat):
    cur.execute("select id_cat from categories where name = %(docCat)s", {'docCat': docCat})
    recset = cur.fetchone()

    if recset:
       catId = recset['id_cat']
    else:
       print("Invalid category. Restart the program.")
       exit()

    sql = "Insert into documents (doc, text, title, num_chars, date, id_cat) Values (%s, %s, %s, %s, %s, %s)"
    values = [docId, docText, docTitle,  len(convertText(docText).replace(" ","")), docDate, catId]
    cur.execute(sql, values)

    dic_Terms = {}
    docText = convertText(docText)
    terms = docText.split(" ")

    for term in terms:
        if dic_Terms.get(term) is None:
           dic_Terms[term] = 1

           cur.execute("select term from terms where term = %(term)s", {'term': term})
           recset = cur.fetchall()

           if not recset:
              sql = "Insert into terms (term, num_chars) Values (%s, %s)"
              values = [term, len(term)]
              cur.execute(sql, values)

        else:
           dic_Terms[term] += 1

    terms = set(terms)
    for term in terms:
        sql = "Insert into index (doc, term, term_count) Values (%s, %s, %s)"
        values = [docId, term, dic_Terms[term]]
        cur.execute(sql, values)

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):
    deleteDocument(cur, docId)
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def deleteDocument(cur, docId):
    cur.execute("select term from index where doc = %(docId)s", {'docId': docId})
    recset = cur.fetchall()

    for term in recset:
        sql = "Delete from index where doc = %(docId)s and term = %(term)s"
        cur.execute(sql, {'docId': docId, 'term': term['term']})
        cur.execute("select term from index where term = %(term)s", {'term': term['term']})
        recset2 = cur.fetchall()
        if not recset2:
           sql = "Delete from terms where term = %(term)s"
           cur.execute(sql, {'term': term['term']})
    sql = "Delete from documents where doc = %(docId)s"
    cur.execute(sql, {'docId': docId})

def getIndex(cur):
    dicIndex = {}
    cur.execute("SELECT index.term, index.term_count, documents.title from index inner join documents on index.doc = documents.doc " 
                "order by index.term, index.term_count")
    recset = cur.fetchall()
    for rec in recset:
        if dicIndex.get(rec['term']) is None:
           dicIndex[rec['term']] = rec['title'] + ":" + str(rec['term_count'])
        else:
           dicIndex[rec['term']] += ", " + rec['title'] + ":" + str(rec['term_count'])

    return dicIndex

def convertText(text):
    text = text.lower()
    text = text.replace(",","")
    text = text.replace(".", "")
    text = text.replace("!", "")
    text = text.replace("?", "")
    return text