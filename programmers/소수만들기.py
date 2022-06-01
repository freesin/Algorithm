
#https://programmers.co.kr/learn/courses/30/lessons/12977
from itertools import combinations, permutations

# def solution(nums):
#     answer = list(combinations(nums, 3))
#     for i in
#     return a
#
# print(solution([1,2,3,4]))
nums = [1,2,3,4]
answer = list(combinations(nums, 3))
a = []
for i in answer:
    a.append(i[0] + i[1] + i[2])
a = set(a)
a = list(a)
print(a)
def isPrimeNumber(a):
    flag = True
    for i in range(2, a + 1):
        if a % i == 0:
            flag = False
    return flag

result = 0
for i in a:
    print(isPrimeNumber(i))
    print(i)
    if not isPrimeNumber(i):
        result += 1


print(result)
