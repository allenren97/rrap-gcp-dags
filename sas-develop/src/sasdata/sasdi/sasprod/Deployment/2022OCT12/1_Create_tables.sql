/* NOTE: Please grant permissions on new tables as mentioned below-
usr_edl_airb, usr_web_apps : INSERT/UPDATE/DELETE permissions  
usr_b9xf_sas, usr_rrap_frg : SELECT permissions */

CREATE TABLE AIRB_RECON.dbo.Online_Users_Info (
	UserID varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	SID varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	UserName varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	Email varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	UserType varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	Active varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	ReqNum varchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	LastAction varchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	ActionDate date NULL,
	Remarks varchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);

CREATE TABLE AIRB_RECON.dbo.Online_Applications_Info (
	Application_Id int NOT NULL,
	Entitlement varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	Functions varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	Privileged varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	Active varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	Opposite_App_id int NULL,
	Remarks varchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);

CREATE TABLE BASEL_DATAFEEDS.dbo.BASEL_Online_Users_Info (
	UserID varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	SID varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	UserName varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	Email varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	UserType varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	Active varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	ReqNum varchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	LastAction varchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	ActionDate date NULL,
	Remarks varchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);

CREATE TABLE BASEL_DATAFEEDS.dbo.BASEL_Online_Applications_Info (
	Application_Id int NOT NULL,
	Entitlement varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	Functions varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	Privileged varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	Active varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	Opposite_App_id int NULL,
	Remarks varchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);

