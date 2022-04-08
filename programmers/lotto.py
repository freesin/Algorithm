#https://programmers.co.kr/learn/courses/30/lessons/77484?language=python3

# lottos	win_nums	result
# [44, 1, 0, 0, 31, 25]	[31, 10, 45, 1, 6, 19]	[3, 5]
# [0, 0, 0, 0, 0, 0]	[38, 19, 20, 40, 15, 25]	[1, 6]
# [45, 4, 35, 20, 3, 9]	[20, 9, 3, 45, 4, 35]	[1, 1]

def solution(lottos, win_nums):
    answer = []
    zero_count = 0
    same_count = 0
    rank_list = {'0': 6, '1': 6, '2': 5, '3': 4, '4': 3, '5': 2, '6': 1}
    for i in lottos:
        if i == 0:
            zero_count += 1
        for j in win_nums:
            if i == j:
                same_count += 1
    answer.append(rank_list[str(same_count + zero_count)])
    answer.append(rank_list[str(same_count)])
    return answer


def solution1(lottos, win_nums):    #조금더 발전 된 소스
    answer = []
    same_count = 0
    rank_list = {'0': 6, '1': 6, '2': 5, '3': 4, '4': 3, '5': 2, '6': 1}
    zero_count = lottos.count(0)
    print('0개수 ' + str(zero_count))
    for i in win_nums:
        if i in lottos:
            same_count += 1
    answer.append(rank_list[str(same_count + zero_count)])
    answer.append(rank_list[str(same_count)])
    return answer


def solution2(lottos, win_nums):    #참고 소스

    rank=[6,6,5,4,3,2,1]

    cnt_0 = lottos.count(0)
    ans = 0
    for x in win_nums:
        if x in lottos:
            ans += 1
    return rank[cnt_0 + ans],rank[ans]

lottos = [45, 4, 35, 20, 3, 9]
win_nums = [20, 9, 3, 45, 4, 35]
print(solution(lottos, win_nums))
