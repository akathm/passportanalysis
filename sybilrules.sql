CREATE TABLE beta_rounds.coefficient_--roundslug
(
    id text
     , projectId text
     , applicationId text
     , roundId text
     , token varchar(3)
     , voter varchar(45)
     , grantAddress varchar(45)
     , amount bigint
     , amountUSD float
     , coefficient float
     , status varchar(6)
     , last_score_timestamp timestamp
     , type text
     , success boolean
     , rawScore float
     , threshold float
)


WITH all_votes AS (
    SELECT
        trim(lower(oss.voter)) AS voter,
        oss.projectid,
        oss.roundid,
        oss.grantaddress,
        oss.amountusd,
        oss.coefficient,
        oss.rawscore,
        oss.success
    FROM
        beta_rounds.coefficient_oss AS oss
    UNION ALL
    SELECT
        trim(lower(zk.voter)) AS voter,
        zk.projectid,
        zk.roundid,
        zk.grantaddress,
        zk.amountusd,
        zk.coefficient,
        zk.rawscore,
        zk.success
    FROM
        beta_rounds.coefficient_zk AS zk
    UNION ALL
    SELECT
        trim(lower(climate.voter)) AS voter,
        climate.projectid,
        climate.roundid,
        climate.grantaddress,
        climate.amountusd,
        climate.coefficient,
        climate.rawscore,
        climate.success
    FROM
        beta_rounds.coefficient_climate AS climate
    UNION ALL
    SELECT
        trim(lower(community.voter)) AS voter,
        community.projectid,
        community.roundid,
        community.grantaddress,
        community.amountusd,
        community.coefficient,
        community.rawscore,
        community.success
    FROM
        beta_rounds.coefficient_community AS community
    UNION ALL
    SELECT
        trim(lower(ethinfra.voter)) AS voter,
        ethinfra.projectid,
        ethinfra.roundid,
        ethinfra.grantaddress,
        ethinfra.amountusd,
        ethinfra.coefficient,
        ethinfra.rawscore,
        ethinfra.success
    FROM
        beta_rounds.coefficient_ethinfra AS ethinfra
)
, recent_attack as ( --312 excluded here
                    select voter
                    from all_votes
                    where voter in (select address from beta_rounds.past_sybils))
, block_frequency as (
                select a.blocknumber
                     , count(distinct a.id) as n_votes_per_block
                from (select blocknumber, id from beta_rounds.votes_oss
                            UNION ALL
                       select blocknumber, id from beta_rounds.votes_zk
                            UNION ALL
                       select blocknumber, id from beta_rounds.votes_climate
                            UNION ALL
                       select blocknumber, id from beta_rounds.votes_community
                            UNION ALL
                       select blocknumber, id from beta_rounds.votes_ethinfra) A
                GROUP BY 1
                ORDER BY 2 desc
                )
, susp_bots as ( --1,141 excluded here
            select distinct
                voter
            from block_frequency
            left join (select voter, blocknumber from beta_rounds.votes_oss
                    UNION select voter, blocknumber from beta_rounds.votes_zk
                    UNION select voter, blocknumber from beta_rounds.votes_climate
                    UNION select voter, blocknumber from beta_rounds.votes_community
                    UNION select voter, blocknumber from beta_rounds.votes_ethinfra) b
            ON b.blocknumber = block_frequency.blocknumber
            where n_votes_per_block > 44
    )
, passport_dup as ( --6743 omitted here
                select distinct voter
                from all_votes
                where voter in (select addresses from passport.alpha_dups)
)
, passport_fail as (
                select trim(lower(voter)) --, rawscore
                from all_votes
                where (success = false or success is null)
                --where voter = '0xa26c5e0fa8babac71dc8de4f95dedd63ec399144'
)
--, final as (
select
    id, projectid, applicationid, roundid, token, trim(lower(voter)) AS voter, grantaddress, amount, amountusd,
    case when trim(lower(voter)) in (select trim(lower(voter)) from beta_rounds.coefficient_connor
                                        where coefficient = 0) then 0
         when trim(lower(voter)) in (select * from recent_attack) then 0
         when trim(lower(voter)) in (select * from susp_bots) then 0
         when trim(lower(voter)) in (select * from passport_dup) then 0
         when trim(lower(voter)) in (select * from passport_fail) then 0
         when trim(lower(voter)) = trim(lower(grantaddress)) then 0
         when (amountusd < .98) then 0
         --when voter = grantaddress then 0
         else 1 end as coefficient
    , status, last_score_timestamp, type, success, rawscore, threshold
from beta_rounds.coefficient_oss
--from beta_rounds.coefficient_zk
--from beta_rounds.coefficient_climate
--from beta_rounds.coefficient_community
--from beta_rounds.coefficient_ethinfra
--from beta_rounds.coefficient_tectoken
)
--select * from final where trim(lower(voter)) = trim(lower('0x6C320A427a22012486722CF8e8b504aC1C0f3B2a'))
/*select projectid, applicationid, updated_coefficient, sum(amountusd)
from final
group by 1, 2, 3*/
select
    updated_coefficient
    , ROUND(percentile_cont(0.25) WITHIN GROUP (ORDER BY rawscore)::numeric, 2) as "25th"
    , ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY rawscore)::numeric, 2) as median
    , ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY rawscore)::numeric, 2) as "75th"
from final
where rawscore > 15.0
group by 1
