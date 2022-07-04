#https://programmers.co.kr/learn/courses/30/lessons/42885?language=python3
from itertools import combinations

def solution(people, limit):
    weights = list(combinations(people, 2))
    tmp = []
    for i in weights:
        if limit >= sum(i):
            if i[0] in people:
                people.remove(i[0])
            if i[1] in people:
                people.remove(i[1])
    answer = 0

    return people


print(solution([70, 50, 80, 50], 100))

# people	limit	return
# [70, 50, 80, 50]	100	3
# [70, 80, 50]	100	3