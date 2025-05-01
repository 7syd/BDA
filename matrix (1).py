def mapper(A, B, m, n, p):
    return [((i, k), ("A", j, A[i][j])) for i in range(m) for j in range(n) for k in range(p)] + \
           [((i, k), ("B", j, B[j][k])) for j in range(n) for k in range(p) for i in range(m)]

def shuffle_and_sort(mapped):
    from collections import defaultdict
    grouped = defaultdict(list)
    for key, val in mapped:
        grouped[key].append(val)
    return grouped

def reducer(shuffled, n):
    result = {}
    for key, values in shuffled.items():
        A, B = {}, {}
        for mat, j, val in values:
            (A if mat == "A" else B)[j] = val
        result[key] = sum(A[j] * B[j] for j in range(n) if j in A and j in B)
    return result


A = [[1, 2], [3, 4]]
B = [[5, 6], [7, 8]]
m, n, p = len(A), len(A[0]), len(B[0])


print("Matrix A:")
for row in A:
    print(row)


print("\nMatrix B:")
for row in B:
    print(row)


mapped = mapper(A, B, m, n, p)
shuffled = shuffle_and_sort(mapped)
result = reducer(shuffled, n)


print("\nResultant Matrix C:")
for (i, k), val in sorted(result.items()):
    print(f"C[{i}][{k}] = {val}")
