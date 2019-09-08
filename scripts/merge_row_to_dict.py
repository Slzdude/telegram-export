data1 = """
forward.date.timestamp(),
forward.from_id,
forward.channel_post,
forward.post_author
"""

data2 = [
    'id', 'original_date', 'from_id', 'channel_post', 'post_author'
]

data1 = data1.strip().splitlines()

for i, j in zip(data2, data1):
    print(f"'{i}':{j}")
