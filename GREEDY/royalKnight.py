n = input()
x = {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5, 'f': 6, 'g': 7, 'h': 8}
x_axis = x[n[:1]]
y_axis = int(n[1:])
move_list = [[-2, -1], [-2, 1], [2, -1], [2, 1], [-1, 2], [-1, 2], [1, -2], [-1, -2]]
ans = 0
for i in move_list:
    x_axis -= i[0]
    y_axis -= i[1]
    if x_axis < 1 or y_axis < 1 or x_axis > 8 or y_axis > 8:
        x_axis = x[n[:1]]
        y_axis = int(n[1:])
    else:
        ans += 1
        x_axis = x[n[:1]]
        y_axis = int(n[1:])
print(ans)
