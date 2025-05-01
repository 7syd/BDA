def mapper(matrix_A, matrix_B, m, n, p):
    mapped_results = []

    # Emit key-value pairs for matrix A
    for i in range(m):
        for j in range(n):
            for k in range(p):
                mapped_results.append(((i, k), ("A", j, matrix_A[i][j])))  # Fixed missing parenthesis

    # Emit key-value pairs for matrix B
    for j in range(n):
        for k in range(p):
            for i in range(m):
                mapped_results.append(((i, k), ("B", j, matrix_B[j][k])))  # Fixed missing parenthesis

    return mapped_results

def shuffle_and_sort(mapped_results):
    shuffled_data = {}

    # Group values by key (i, k)
    for key, value in mapped_results:
        if key in shuffled_data:
            shuffled_data[key].append(value)
        else:
            shuffled_data[key] = [value]

    return shuffled_data


def reducer(shuffled_data, n):
    result_matrix = {}

    # Compute the dot product for each key (i, k)
    for key, values in shuffled_data.items():
        i, k = key
        A = {}
        B = {}

        # Populate A and B dictionaries
        for value in values:
            matrix, j, val = value
            if matrix == "A":
                A[j] = val
            elif matrix == "B":
                B[j] = val

        # Compute the dot product
        result = 0
        for j in range(n):
            if j in A and j in B:
                result += A[j] * B[j]

        # Store the result in the result matrix
        result_matrix[(i, k)] = result

    return result_matrix


if __name__ == "__main__":
    matrix_A = [
        [1, 2],
        [3, 4]
    ]
    matrix_B = [
        [5, 6],
        [7, 8]
    ]





    m = len(matrix_A)  # Rows in A
    n = len(matrix_A[0])  # Columns in A and rows in B
    p = len(matrix_B[0])  # Columns in B



    mapped_results = mapper(matrix_A, matrix_B, m, n, p)

    shuffled_data = shuffle_and_sort(mapped_results)

    result_matrix = reducer(shuffled_data, n)



    print("Resultant Matrix C:")
    for (i, k), value in result_matrix.items():
        print(f"C[{i}][{k}] = {value}")
