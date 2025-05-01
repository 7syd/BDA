import csv
import pandas as pd

sample_data = [
    ['StudentID', 'Name', 'Marks'],
    ['101', 'Alice', '95'],
    ['102', 'Bob', '82'],
    ['103', 'Charlie', '73'],
    ['104', 'David', '67'],
    ['105', 'Eva', '58']
]

# Save to CSV
with open('grade1.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerows(sample_data)

# Read CSV into DataFrame
df_original = pd.read_csv('grade.csv')
df_original

def mapper(dataframe):
    return [(row['StudentID'], row['Name'], int(row['Marks'])) for _, row in dataframe.iterrows()]

def reducer(mapped_data):
    reduced_data = []
    for student_id, name, marks in mapped_data:
        if marks >= 90: grade = 'A'
        elif marks >= 80: grade = 'B'
        elif marks >= 70: grade = 'C'
        elif marks >= 60: grade = 'D'
        else: grade = 'F'
        reduced_data.append((student_id, name, marks, grade))a
    return reduced_data

mapped = mapper(df_original)
reduced = reducer(mapped)

# Convert reduced data to DataFrame
df_result = pd.DataFrame(reduced, columns=['StudentID', 'Name', 'Marks', 'Grade'])
df_result
