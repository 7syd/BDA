def mapper(text):
    return [(word.lower(), 1) for word in text.split()]

def shuffle_and_sort(mapped):
    from collections import defaultdict
    grouped = defaultdict(list)
    for word, count in mapped:
        grouped[word].append(count)
    return grouped

def reducer(grouped):
    return {word: sum(counts) for word, counts in grouped.items()}

if __name__ == "__main__":
    input_text = """
    Hello world
    Hello sheeesh
    MapReduce is powerful
    Hello again
    Bye Bye
    """

    mapped = [pair for line in input_text.strip().split('\n') for pair in mapper(line)]
    grouped = shuffle_and_sort(mapped)
    result = reducer(grouped)

    print("Word Frequency:")
    for word in sorted(result):
        print(f"{word}: {result[word]}")
