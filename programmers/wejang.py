# [["yellowhat", "headgear"], ["bluesunglasses", "eyewear"], ["green_turban", "headgear"]]	5
# [["crowmask", "face"], ["bluesunglasses", "face"], ["smoky_makeup", "face"]]	3

clothes = [["yellowhat", "headgear"], ["bluesunglasses", "eyewear"], ["green_turban", "headgear"]]

def solution(clothes):
    answer = 0
    cloths = {}

    for name, kind in clothes:
        if kind in cloths:
            cloths[kind] += 1
        else:
            cloths[kind] = 1

    answer = 1
    print(cloths)
    for key, value in cloths.items():
        print(key, value)
        answer *= (value + 1)

    return answer - 1
print(solution(clothes))
