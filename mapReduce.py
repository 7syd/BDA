import csv
import pandas as pd

with open('grade.csv', 'r') as file:
    reader = csv.DictReader(file)
    original_data = list(reader)

print("Original Dataset:\n")
print("StudentID\tName\t\tMarks")
for row in original_data:
    print(f"{row['StudentID']}\t\t{row['Name']}\t\t{row['Marks']}")

def mapper(data):
    mapped_data = []
    for row in data:
        student_id = row['StudentID']
        name = row['Name']
        marks = int(row['Marks'])
        mapped_data.append((student_id, name, marks))
    return mapped_data

def reducer(mapped_data):
    reduced_data = []
    for student_id, name, marks in mapped_data:
        if marks >= 90:
            grade = 'A'
        elif marks >= 80:
            grade = 'B'
        elif marks >= 70:
            grade = 'C'
        elif marks >= 60:
            grade = 'D'
        else:
            grade = 'F'
        reduced_data.append((student_id, name, marks, grade))
    return reduced_data

mapped = mapper(original_data)
reduced = reducer(mapped)

print("\n  Dataset with Grades:\n")
print("StudentID\tName\t\tMarks\tGrade")
for student_id, name, marks, grade in reduced:
    print(f"{student_id}\t\t{name}\t\t{marks}\t{grade}")
