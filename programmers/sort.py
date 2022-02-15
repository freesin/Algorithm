#[1, 5, 2, 6, 3, 7, 4]	[[2, 5, 3], [4, 4, 1], [1, 7, 3]]	[5, 6, 3]

commands = [[2, 5, 3], [4, 4, 1], [1, 7, 3]]
array = [1, 5, 2, 6, 3, 7, 4]
print(''.join(str(e) for e in array))

def solution(array, commands):
    a = ''
    for i in array:
        a += str(i)
    return a

print(solution(array, commands))
 