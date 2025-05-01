text = '''I like Hadoop.
          Hadoop is good for big data computing.
          MapReduce is part of Hadoop.
          This is an example of MapReduce.
        '''



def mapper(text):
    result = []
    words = text.split()
    for word in words:
        word = word.strip('.,!?').lower()
        result.append((word,1))

    return result




map_output = mapper(text)
print("Result: ", map_output)





def shuffle_sort(mp_output):
    result = {}
    for word,count in mp_output:
        if word in result:
            result[word].append(count)
        else:
            result[word] = [count]
    
    return result




def shuffle_sort(mp_output):
    result = {}
    for word,count in mp_output:
        if word in result:
            result[word].append(count)
        else:
            result[word] = [count]
    
    return result


shuffle_sort_output = shuffle_sort(map_output)
print("Shuffle Sort Output: ", shuffle_sort_output)




def reducer(shuffle_sort_output):
    result = {}

    for word, counts in shuffle_sort_output.items():
        result[word] = sum(counts)
    return result



reducer_output = reducer(shuffle_sort_output)
print("Reducer output is: ", reducer_output)
