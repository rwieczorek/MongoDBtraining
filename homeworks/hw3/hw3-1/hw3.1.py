
import pymongo
import operator

# establish a connection to the database
connection = pymongo.MongoClient("mongodb://localhost")

# get a handle to the students database
db=connection.school
students = db.students

#
def remove_poor_homework_grades():

    print "remove_poor_homework_grades(), reporting for duty"

    try:
        # find nearest homework score for each student and flag it for deletion
        print "set query..."
        query = {}
        #print "set projection..."
        #projection = {'student_id':1, '_id':0}
        print "run query..."
        #docs = grades.find(query).sort([('student_id', pymongo.DESCENDING),('score', pymongo.DESCENDING)])
        students_collection = students.find(query)

        for student in students_collection:

            student_id = student['_id']

            print "Getting scores for student _id=" + str(student_id)
            student_scores = student['scores']
            new_scores=[]
            homework_scores=[]

            for score in student_scores:
                if score['type'] != 'homework':
                    new_scores.append(score)
                else:
                    homework_scores.append(score)

            from operator import itemgetter
            homework_scores_sorted = sorted(homework_scores, key=itemgetter('score'), reverse=True)
            no_of_homework_scores = len(homework_scores_sorted)

            i = 0
            while no_of_homework_scores-1 >= i:
                if i < no_of_homework_scores-1:
                    new_scores.append(homework_scores_sorted[i])
                i = i + 1

            print "Print new scores for student _id=" + str(student_id)
            for new_score in new_scores:
                print new_score

            print "Update the scores in MongoDB for student _id=" + str(student_id)
            result = students.update_one({'_id': student_id},
                      {'$set': {'scores': new_scores}})
            print "num updates: ", result.matched_count

    except Exception as e:
        print "Unexpected error:", type(e), e


if __name__ == '__main__':
    remove_poor_homework_grades()
