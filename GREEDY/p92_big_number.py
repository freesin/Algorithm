
N, M, K = map(int, input().split())
data = list(map(int, input().split()))
data.sort()

num1 = data[-1]
num2 = data[-2]
ans = 0
count = 1
for i in range(M):
    if count < K:
        count += 1
        ans += num1
    else:
        ans += num2
        count = 0

print(ans)
# 나의 풀이 단순한 풀이 -> 100억개 이상 넘어가면 풀이가 달라져야한다.

#반복되는 수열을 파악해야한다.
#점화식 활용 int ( M / ( K + 1) ) * K + M % (K + 1)


# 점화식 활용

calc = int(M / (K + 1)) * K
calc += M % (K + 1)

result = 0
result += (calc) * num1
result += (M - calc) * num2
print(result)
