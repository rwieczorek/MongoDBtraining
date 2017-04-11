
import pymongo

# establish a connection to the database
connection = pymongo.MongoClient("mongodb://localhost")

# get a handle to the students database
db=connection.students
grades = db.grades

#
def remove_poor_homework_grades():

    print "remove_poor_homework_grades(), reporting for duty"

    try:
        # find nearest homework score for each student and flag it for deletion
        print "set query..."
        query = {'type': 'homework'}
        #print "set projection..."
        #projection = {'student_id':1, '_id':0}
        print "run query..."
        docs = grades.find(query).sort([('student_id', pymongo.DESCENDING),('score', pymongo.DESCENDING)])

        current_student_id = -1
        prev_student_id = -1
        
        for doc in docs:
            current_student_id = doc['student_id']
            id=doc['_id']
            if current_student_id == prev_student_id:
                result = grades.delete_many({'_id':id})
                print "num removed: ", result.deleted_count
            prev_student_id = current_student_id

    except Exception as e:
        print "Unexpected error:", type(e), e


if __name__ == '__main__':
    remove_poor_homework_grades()
