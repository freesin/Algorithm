n, m = map(int, input().split())

#캐릭터 방문위치 확인용 맵
d = [[0] * m for _ in range(n)]

x, y, direction = map(int, input().split())

#현재 캐릭터의 좌표 위치
d[x][y] = 1

array = []
for i in range(n):
    array.append(list(map(int, input().split())))

dx = [-1, 0, 1, 0]
dy = [0, 1, 0, -1]

#왼쪽으로 회전
def turn_left():
    global direction
    direction -= 1
    if direction == -1:
        direction = 3

count = 1
turn_time = 0
while True:
    turn_left()
    nx = x + dx[direction]
    ny = y + dy[direction]

# 북 : 0 (-1, 0) 이동  방향 3
# 동 : 1 (0, 1) 이동   방향 0
# 남 : 2 (1, 0) 이동   방향 1
# 서 : 3 (0, -1) 이동  방향 2

# board = []
# cnt = 1
# for i in range(n):
#     board.append(list(map(int, input().split())))
#
# for i in board:
#     for j in board:
#         d[i][j] = board[i][j]
#
# print(board)
print(d)



# 1. 주어진 위치 기준 갈 방향 정하기
# 2. 안 가본칸 왼쪽으로 회전 후 왼쪽으로 한칸 전진 / 안가본칸 없으면 회전만하고 1단계
# 3. 모두 가본 칸이면 방향 유지 후 한 칸 뒤로 (뒤가 바다인 칸이면 움직임 stop)