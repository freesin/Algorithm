def solution(stones, k):
    answer = ''
    return answer

stones = list(map(int, input().split()))
k = int(input())
tmp_stones = stones
for i in tmp_stones:
    print(i)

def action(arr, plus_idx):
    for i in range(len(arr)):
        arr[i] -= 1
    arr[plus_idx] += 2
    return arr

while True:
    plus_idx = 0




# 돌무더기 게임은 여러 돌무더기에서 돌을 하나씩 추가하거나 제거하여, 하나의 돌무더기만 남기는 게임입니다. 게임에서 승리하기 위해서는 마지막에 남은 하나의 돌무더기의 돌 수가 정확히 k개가 되어야 합니다.
# 처음 주어지는 돌무더기들은 각각 1개 이상의 돌을 갖고 있습니다. 당신은 한 돌무더기를 골라 다음과 같은 행동을 할 수 있습니다.
# * 선택한 돌무더기에 돌을 1개 추가합니다. 동시에, 선택되지 않은 나머지 돌무더기에서 각각 1개씩 돌을 제거합니다.
# * 돌을 제거해야 할 나머지 돌무더기 중에서 제거할 돌이 없는 경우(=돌 수가 0인 경우), 위 행동은 실행할 수 없습니다.
# 예를 들어 3개의 돌무더기에 돌이 각각 [1, 3, 2]개 있고 k가 3인 경우, 게임에서 승리하는 가장 빠른 방법은 아래와 같이 2가지가 있습니다.
# 1. 첫 번째 - 세 번째 - 세 번째 돌무더기를 차례대로 선택합니다. 각 선택 시에 돌무더기의 돌 수는 [2, 2, 1] - [1, 1, 2] - [0, 0, 3]과 같이 변합니다.
# 2. 세 번째 - 첫 번째 - 세 번째 돌무더기를 차례대로 선택합니다. 각 선택 시에 돌무더기의 돌 수는 [0, 2, 3] - [1, 1, 2] - [0, 0, 3]과 같이 변합니다.
# 선택하는 돌무더기의 인덱스를 순서대로 나열하여 문자열로 변환하면 1번 방법은 "022", 2번 방법은 "202"입니다. 이를 사전 순으로 정렬했을 때, 가장 뒤에 오는 방법은 "202"입니다.
# 각 돌무더기의 돌 수를 나타내는 정수 배열 stones, 남겨야 하는 한 돌무더기의 돌 수를 나타내는 정수 k가 매개변수로 주어집니다. 게임에서 승리하기 위한 가장 빠른 방법 중에서, 선택하는 돌무더기의 인덱스를 문자열로 변환했을 때 사전 순으로 가장 뒤에 오는 방법을 return 하도록 solution 함수를 완성해주세요. 만약 어떤 방법으로도 목표를 달성할 수 없다면 "-1"을 return 해주세요.
#
# 제한사항
# * 2 ≤ stones의 길이 ≤ 8
#     * 1 ≤ stones의 원소 ≤ 12
# * 1 ≤ k ≤ 24
# * 돌 수가 0인 돌무더기도 선택할 수 있는 돌무더기입니다.
