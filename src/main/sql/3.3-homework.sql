with tmp as (
  SELECT
    user_id,
    click_time,
    sum(flag) over (
      PARTITION BY user_id
      ORDER BY
        click_time
    ) sumflag
  FROM
    (
      SELECT
        user_id,
        click_time,
        CASE
          WHEN sub_min >= 30 THEN 1
          ELSE 0
        END flag
      FROM
        (
          SELECT
            user_id,
            click_time,
            (
              unix_timestamp(click_time) - unix_timestamp(
                lag (click_time) over (
                  PARTITION BY user_id
                  ORDER BY
                    click_time
                )
              )
            ) / 60 sub_min
          FROM
            user_clicklog
        ) t1
    ) t2
)
select
  user_id,
  click_time,
  row_number() over(
    partition by user_id,
    sumflag
    order by
      click_time
  ) index
from
  tmp
