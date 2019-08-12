-- 这是没有变化的数据
-- 取变化之前的，备份起来
select aaa.*,  aaa.act_start, currentdate as act_end
from aaa
left join bbb
on aaa.id = bbb.id
where
       ( ( aaa.a is null xor bbb.a is null ) or ( aaa.a <> bbb.a ) )
    or ( ( aaa.b is null xor bbb.b is null ) or ( aaa.b <> bbb.b ) )
    or ( ( aaa.c is null xor bbb.c is null ) or ( aaa.c <> bbb.c ) )
    or ( ( aaa.d is null xor bbb.d is null ) or ( aaa.d <> bbb.d ) )



-- 这是有变化的数据
-- 取变化之后的，新插入
select bbb.*, currentdate as act_start, 99990101 as act_end
from aaa
left join bbb
on aaa.id = bbb.id
where
       ( ( aaa.a is null xor bbb.a is null ) or ( aaa.a <> bbb.a ) )
    or ( ( aaa.b is null xor bbb.b is null ) or ( aaa.b <> bbb.b ) )
    or ( ( aaa.c is null xor bbb.c is null ) or ( aaa.c <> bbb.c ) )
    or ( ( aaa.d is null xor bbb.d is null ) or ( aaa.d <> bbb.d ) )


-- 这是没有变化的数据
select aaa.*, aaa.act_start, aaa.act_end
from aaa
left join bbb
on aaa.id = bbb.id
where
        ( ( aaa.a is null and bbb.a is null ) or ( aaa.a = bbb.a ) )
    and ( ( aaa.b is null and bbb.b is null ) or ( aaa.b = bbb.b ) )
    and ( ( aaa.c is null and bbb.c is null ) or ( aaa.c = bbb.c ) )
    and ( ( aaa.d is null and bbb.d is null ) or ( aaa.d = bbb.d ) )










