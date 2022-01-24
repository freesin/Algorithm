from itertools import permutations


def solution(numbers):
    result = []
    for i in permutations(numbers, len(numbers)):
        temp = ''
        for q in i:
            temp += str(q)
        result.append(int(temp))
    answer = str(max(result))

    return answer

#https://programmers.co.kr/learn/courses/30/lessons/42746?language=javascript