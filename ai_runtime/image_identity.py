"""Current, group-scoped identity evidence accompanying an image tool result."""

SQL = '''SELECT m.steamid,m.uid,p.name AS steam_nickname,p.updated_at AS steam_updated_at,
    g.nickname AS cached_qq_nickname,g.updated_at AS qq_updated_at
    FROM members m LEFT JOIN profiles p ON p.steamid=m.steamid
    LEFT JOIN group_people g ON g.uid=m.uid ORDER BY m.steamid,m.uid'''


async def with_image_identities(image, broker, group):
    if image.get('storage') != 'snapshot':
        return image
    context = {
        'basis': 'current_group_bindings',
        'note': '按SteamID关联QQ身份；昵称不是身份键。这是读取时的绑定和名字，不是发图时的历史绑定。未绑定、缺失或多重绑定不能猜人；旧metadata若只有昵称则不能凭此认定身份。',
    }
    try:
        result = await broker.query(SQL)
    except Exception:
        context.update(status='unavailable', bindings=[], truncated=False)
        return image | {'identity_context': context}
    try:
        members = {row['uid']: row for row in await group.members()}
        context.update(qq_names_source='OneBot get_group_member_list', qq_checked_at=group._members_checked)
    except Exception:
        members = None
        context.update(qq_names_source='database_cache', qq_checked_at=None)
    bindings = []
    for row in result['rows']:
        current = members.get(row['uid']) if members is not None else None
        bindings.append(row | {
            'qq_nickname': current['nickname'] if current else row['cached_qq_nickname'],
            'qq_card': current['card'] if current else None,
            'in_current_qq_group': row['uid'] in members if members is not None else None,
        })
    context.update(status='available', bindings=bindings, truncated=result['truncated'],
                   fetched_at=result['fetched_at'], source=result['source'])
    return image | {'identity_context': context}
