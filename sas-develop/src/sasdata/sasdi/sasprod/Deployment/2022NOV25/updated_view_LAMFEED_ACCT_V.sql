/* Connect to AIRB_RECON database, and alter view */
/* provide SELECT access to all usr* users (usr_b9xf_sas, usr_edl_airb, usr_rrap_frg, usr_web_apps) */

--------------------------------------------------FOR ACCOUNT FEED-------------------------

use AIRB_RECON;

alter view dbo.LAMFEED_ACCT_V as
select --* 
ui.userid as id, 'GID' as corrIdType, ui.sid as ScotiaID, ui.username as userName, '' as description, 
ual.entitlement_list as groups, '' as profiles, '' as functions, '' as tables,
ui.active as active, 
privileged= case 
when 'Y' in (select ai.Privileged from AIRB_RECON.dbo.Online_Users u, AIRB_RECON.dbo.Online_Applications_Info ai 
	where u.application_id=ai.application_id and u.User_Name=ud.User_Name) 
then 'Y' else 'N' end,
 '' as accountType, '' as lastLogin, '' as email, '' as fullName, '' as instance
from (select distinct user_name from AIRB_RECON.dbo.Online_Users) ud,
AIRB_RECON.dbo.Online_Users_Info ui,
(
SELECT 
  t.userid, 
  entitlement_list = STUFF((
    SELECT ',' + t2.entitlement
    FROM (select --* 
ui.userid , ai.entitlement from AIRB_RECON.dbo.Online_Users u,
AIRB_RECON.dbo.Online_Applications a,
AIRB_RECON.dbo.Online_Users_Info ui,
AIRB_RECON.dbo.Online_Applications_Info ai
where u.application_id=a.application_id and u.user_name=ui.userid and a.application_id=ai.application_id
and ui.active='Y' and ai.active='Y'
) t2
    WHERE t.userid = t2.userid
    FOR XML PATH('')),1,1,'')
FROM (select --* 
ui.userid , ai.entitlement , ai.privileged from AIRB_RECON.dbo.Online_Users u,
AIRB_RECON.dbo.Online_Applications a,
AIRB_RECON.dbo.Online_Users_Info ui,
AIRB_RECON.dbo.Online_Applications_Info ai
where u.application_id=a.application_id and u.user_name=ui.userid and a.application_id=ai.application_id
and ui.active='Y' and ai.active='Y'
) as t
GROUP BY t.userid
) ual
where ud.user_name=ui.userid and ud.user_name=ual.userid and ui.active='Y'

union all

select --* 
ui.userid as id, 'GID' as corrIdType, ui.sid as corrId, ui.username as userName, '' as description, 
ual.entitlement_list as groups, '' as profiles, '' as functions, '' as tables,
ui.active as active, 
privileged= case 
when 'Y' in (select ai.Privileged from BASEL_DATAFEEDS.dbo.BASEL_Online_Users u, BASEL_DATAFEEDS.dbo.BASEL_Online_Applications_Info ai 
	where u.application_id=ai.application_id and u.BNSName=ud.BNSName) 
then 'Y' else 'N' end,
 '' as accountType, '' as lastLogin, '' as email, '' as fullName, '' as instance
from (select distinct BNSName from BASEL_DATAFEEDS.dbo.BASEL_Online_Users) ud,
BASEL_DATAFEEDS.dbo.BASEL_Online_Users_Info ui,
(
SELECT 
  t.userid, 
  entitlement_list = STUFF((
    SELECT ',' + t2.entitlement
    FROM (select --* 
ui.userid , ai.entitlement from BASEL_DATAFEEDS.dbo.BASEL_Online_Users u,
BASEL_DATAFEEDS.dbo.BASEL_Online_Applications a,
BASEL_DATAFEEDS.dbo.BASEL_Online_Users_Info ui,
BASEL_DATAFEEDS.dbo.BASEL_Online_Applications_Info ai
where u.application_id=a.application_id and u.BNSName=ui.userid and a.application_id=ai.application_id
and ui.active='Y' and ai.active='Y'
) t2
    WHERE t.userid = t2.userid
    FOR XML PATH('')),1,1,'')
FROM (select --* 
ui.userid , ai.entitlement , ai.privileged from BASEL_DATAFEEDS.dbo.BASEL_Online_Users u,
BASEL_DATAFEEDS.dbo.BASEL_Online_Applications a,
BASEL_DATAFEEDS.dbo.BASEL_Online_Users_Info ui,
BASEL_DATAFEEDS.dbo.BASEL_Online_Applications_Info ai
where u.application_id=a.application_id and u.BNSName=ui.userid and a.application_id=ai.application_id
and ui.active='Y' and ai.active='Y'
) as t
GROUP BY t.userid
) ual
where ud.BNSName=ui.userid and ud.BNSName=ual.userid and ui.active='Y'
;
