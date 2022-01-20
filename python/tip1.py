from collections import Counter
# 1. 리스트
# : 요소의 개수 파악

lst = ['aa', 'cc', 'dd', 'aa', 'bb', 'ee']
print(Counter(lst))
Counter({'aa': 2, 'cc': 1, 'dd': 1, 'bb': 1, 'ee': 1})

# 2. 딕셔너리
# : value값이 큰 순서대로

print(Counter({'가': 3, '나': 2, '다': 4}))
Counter({'다': 4, '가': 3, '나': 2})

# 3. 값 = 개수 형태

c = Counter(a=2, b=3, c=2)
print(Counter(c))
Counter({'b': 3, 'a': 2, 'c': 2})
print(sorted(c.elements()))

#4. 문자열
container = Counter()
container.update("aabcdeffgg")
print(container)
Counter({'a': 2, 'f': 2, 'g': 2, 'b': 1, 'c': 1, 'd': 1, 'e': 1})

#5. most_common()
#: 개수의 해당하는 값 추출

c2 = 'apple, orange, grape'
c2 = Counter(c2)
print(c2.most_common())

print(c2.most_common(3))
