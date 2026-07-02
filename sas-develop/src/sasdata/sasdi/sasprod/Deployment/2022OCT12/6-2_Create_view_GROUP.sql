/* Connect to AIRB_RECON database, and create view */
/* provide SELECT access to all usr* users (usr_b9xf_sas, usr_edl_airb, usr_rrap_frg, usr_web_apps) */

--------------------------------------------------FOR GROUP FEED-------------------------

create view dbo.LAMFEED_GROUP_V as
select --* 
ai.Entitlement as groupName, a.Application_Name as description, 'Y' as groups, '' as profiles, '' as functions, '' as tables, ai.privileged as privileged
from AIRB_RECON.dbo.Online_Applications a, AIRB_RECON.dbo.Online_Applications_Info ai
where a.Application_Id =ai.Application_Id and ai.Active ='Y'
union all
select --* 
ai.Entitlement as groupName, a.Application_Name as description, 'Y' as groups, '' as profiles, '' as functions, '' as tables, ai.privileged as privileged
from BASEL_DATAFEEDS.dbo.BASEL_Online_Applications a, BASEL_DATAFEEDS.dbo.BASEL_Online_Applications_Info ai
where a.Application_Id =ai.Application_Id and ai.Active ='Y'
;