/* Backout plan */

--drop tables
drop table AIRB_RECON.dbo.Online_Users_Info ;
drop table AIRB_RECON.dbo.Online_Applications_Info;
drop table BASEL_DATAFEEDS.dbo.BASEL_Online_Users_Info ;
drop table BASEL_DATAFEEDS.dbo.BASEL_Online_Applications_Info;

--connect to AIRB_RECON db and drop views
drop view dbo.LAMFEED_ACCT_V ;
drop view dbo.LAMFEED_GROUP_V ;
