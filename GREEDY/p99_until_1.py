n, m = map(int, input().split())
new_n = n
count = 0
while True:
    if n % m == 0:
        n //= m
    else:
        n -= 1
    count += 1
    if n == 1:
        break
print(count)

result = 0
# 책 풀이
while True:
    # N == M 로 나누어 떨어지는 수가 될때 까지 1씩 빼기
    target = (new_n // m) * m
    result += (new_n - target)
    new_n = target
    # N이 M보다 작을 때 (더 이상 나눌 수 없을 때) 반복문 탈출
    if new_n < m:
        break

    # M으로 나누기
    result += 1
    new_n //= m

result += (new_n - 1)
print(result)
