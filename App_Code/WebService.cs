using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using System.Configuration;

/// <summary>
/// WebService 的摘要说明
/// </summary>
[WebService(Namespace = "http://tempuri.org/")]
[WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
// 若要允许使用 ASP.NET AJAX 从脚本中调用此 Web 服务，请取消注释以下行。 
// [System.Web.Script.Services.ScriptService]
public class WebService : System.Web.Services.WebService
{
    DealDate dealDate = new DealDate();
    public WebService()
    {
        //如果使用设计的组件，请取消注释以下行 
        //InitializeComponent(); 
    }

    [WebMethod]
    public string HelloWorld()
    {
        return "Hello World";
    }

    //零隐裂
    [WebMethod]
    public DataTable GetELData()
    {
        string sql = string.Empty, sErrMsg = string.Empty;
        string eldate, elyear, elmonth, elday, elyearweek, elquarter, elmonthweek, elfirstday, elqfirstmonth;
        DateTime dtnow;
        DataTable dtReturn = new DataTable()
;
        dtReturn.TableName = "ELDATA";

        #region//get year month day week quarter
        sql = string.Format(@"select to_char(sysdate,'YYYY-MM-DD') as eldate,to_char(sysdate,'YYYY') as elyear,to_char(sysdate,'MM') as elmonth
                                ,to_char(sysdate,'DD') as elday,to_char(sysdate,'ww') as elyearweek,to_char(sysdate,'Q') as elquarter
                                ,to_char(sysdate,'iw')-to_char(last_day(add_months(sysdate,-1))+1,'iw')+1 as elmonthweek,to_char(trunc(sysdate,'y'),'YYYY-MM-DD') as elfirstday
                                ,to_char(to_date(to_char(trunc(sysdate, 'q'), 'yyyy-mm-dd'),'yyyy-mm-dd'),'MM') as elqfirstmonth
                                from dual");
        DataSet dsdate = dealDate.GetData(sql, out sErrMsg);

        eldate = Convert.ToString(dsdate.Tables[0].Rows[0]["eldate"]);
        elyear = Convert.ToString(dsdate.Tables[0].Rows[0]["elyear"]);
        elmonth = Convert.ToString(dsdate.Tables[0].Rows[0]["elmonth"]);
        elday = Convert.ToString(dsdate.Tables[0].Rows[0]["elday"]);
        elyearweek = Convert.ToString(dsdate.Tables[0].Rows[0]["elyearweek"]);
        elquarter = Convert.ToString(dsdate.Tables[0].Rows[0]["elquarter"]);
        elmonthweek = Convert.ToString(dsdate.Tables[0].Rows[0]["elmonthweek"]);
        elfirstday = Convert.ToString(dsdate.Tables[0].Rows[0]["elfirstday"]);
        elqfirstmonth = Convert.ToString(dsdate.Tables[0].Rows[0]["elqfirstmonth"]);
        dtnow = Convert.ToDateTime(eldate);
        #endregion

        #region//初始化表
        dtReturn.Columns.Add("FACTORY_CODE");
        dtReturn.Columns.Add("FACTORY_NAME");
        for (int i = 1; i <= Convert.ToInt32(elquarter); i++)
        {
            dtReturn.Columns.Add("Q" + Convert.ToString(i));
        }
        for (int i = Convert.ToInt32(elqfirstmonth); i <= Convert.ToInt32(elmonth); i++)
        {
            //dtReturn.Columns.Add("M" + Convert.ToString(i));
            dtReturn.Columns.Add(dealDate.GetMonth(Convert.ToString(i)));
        }
        for (int i = 1; i <= Convert.ToInt32(elmonthweek); i++)
        {
            //dtReturn.Columns.Add("M" + elmonth + "-W" + Convert.ToString(i));
            dtReturn.Columns.Add(dealDate.GetMonth(elmonth) + "-W" + Convert.ToString(i));
        }
        for (int i = 6; i >= 0; i--)
        {
            if (dtnow.AddDays(-i) >= Convert.ToDateTime(elfirstday))
            {
                //dtReturn.Columns.Add("M" + dtnow.AddDays(-i).Month.ToString() + "-D" + dtnow.AddDays(-i).Day.ToString());
                dtReturn.Columns.Add(dealDate.GetMonth(dtnow.AddDays(-i).Month.ToString()) + "-" + dtnow.AddDays(-i).Day.ToString());
            }
        }
        #endregion

        #region //get el quarter data
        sql = string.Format(@"select a.factoryname,a.description,a.elquarter,round((a.eltot-nvl(b.elngtot,0))/a.eltot,4)*100||'%' as elrate
                                from (
                                select t2.factoryname,t2.description,t2.elquarter,count(t2.historyid) as eltot
                                from (
                                select row_number() over(PARTITION BY t1.factoryname,t1.elquarter,t1.historyid ORDER BY t1.txndate DESC) AS rowflag
                                ,t1.factoryname,t1.description,t1.historyid,t1.jkoelresult,t1.elquarter
                                from (
                                select f.factoryname,f.description,his.historyid,his.txndate,el.jkoelresult,to_char(his.txndate,'Q') as elquarter
                                from jkoelmovestdhistory el
                                inner join historymainline his on his.historymainlineid=el.historymainlineid
                                inner join spec s on s.specid=his.specid
                                inner join specbase sb on sb.specbaseid=s.specbaseid
                                inner join factory f on f.factoryid=his.factoryid
                                where sb.specname='EL测试'
                                and his.txndate>=trunc(sysdate,'y')
                                ) t1
                                ) t2
                                where t2.rowflag=1
                                group by t2.factoryname,t2.description,t2.elquarter
                                ) a
                                left join 
                                (select t2.factoryname,t2.description,t2.elquarter,count(t2.historyid) as elngtot
                                from (
                                select row_number() over(PARTITION BY t1.factoryname,t1.elquarter,t1.historyid ORDER BY t1.txndate DESC) AS rowflag
                                ,t1.factoryname,t1.description,t1.historyid,t1.jkoelresult,t1.elquarter
                                from (
                                select f.factoryname,f.description,his.historyid,his.txndate,el.jkoelresult,to_char(his.txndate,'Q') as elquarter
                                from jkoelmovestdhistory el
                                inner join historymainline his on his.historymainlineid=el.historymainlineid
                                inner join spec s on s.specid=his.specid
                                inner join specbase sb on sb.specbaseid=s.specbaseid
                                inner join factory f on f.factoryid=his.factoryid
                                where sb.specname='EL测试'
                                and el.jkoelresult in ('MC-OK','JK隐裂')
                                and his.txndate>=trunc(sysdate,'y')
                                ) t1
                                ) t2
                                where t2.rowflag=1
                                group by t2.factoryname,t2.description,t2.elquarter)  b on  a.factoryname=b.factoryname and a.elquarter=b.elquarter");

        DataSet dsQuarter = dealDate.GetData(sql, out sErrMsg);
        #endregion

        #region//get el month data
        sql = string.Format(@"select a.factoryname,a.description,a.elmonth,round((a.eltot-nvl(b.elngtot,0))/a.eltot,4)*100||'%' as elrate
                                from (
                                select t2.factoryname,t2.description,t2.elmonth,count(t2.historyid) as eltot
                                from (
                                select row_number() over(PARTITION BY t1.factoryname,t1.elmonth,t1.historyid ORDER BY t1.txndate DESC) AS rowflag
                                ,t1.factoryname,t1.description,t1.historyid,t1.jkoelresult,t1.elmonth
                                from (
                                select f.factoryname,f.description,his.historyid,his.txndate,el.jkoelresult,to_char(his.txndate,'MM') as elmonth
                                from jkoelmovestdhistory el
                                inner join historymainline his on his.historymainlineid=el.historymainlineid
                                inner join spec s on s.specid=his.specid
                                inner join specbase sb on sb.specbaseid=s.specbaseid
                                inner join factory f on f.factoryid=his.factoryid
                                where sb.specname='EL测试'
                                and his.txndate>=trunc(sysdate,'y')
                                ) t1
                                ) t2
                                where t2.rowflag=1
                                group by t2.factoryname,t2.description,t2.elmonth
                                ) a
                                left join 
                                (select t2.factoryname,t2.description,t2.elmonth,count(t2.historyid) as elngtot
                                from (
                                select row_number() over(PARTITION BY t1.factoryname,t1.elmonth,t1.historyid ORDER BY t1.txndate DESC) AS rowflag
                                ,t1.factoryname,t1.description,t1.historyid,t1.jkoelresult,t1.elmonth
                                from (
                                select f.factoryname,f.description,his.historyid,his.txndate,el.jkoelresult,to_char(his.txndate,'MM') as elmonth
                                from jkoelmovestdhistory el
                                inner join historymainline his on his.historymainlineid=el.historymainlineid
                                inner join spec s on s.specid=his.specid
                                inner join specbase sb on sb.specbaseid=s.specbaseid
                                inner join factory f on f.factoryid=his.factoryid
                                where sb.specname='EL测试'
                                and el.jkoelresult in ('MC-OK','JK隐裂')
                                and his.txndate>=trunc(sysdate,'y')
                                ) t1
                                ) t2
                                where t2.rowflag=1
                                group by t2.factoryname,t2.description,t2.elmonth)  b on  a.factoryname=b.factoryname and a.elmonth=b.elmonth");

        DataSet dsMonth = dealDate.GetData(sql, out sErrMsg);
        #endregion

        #region//get el month week data
        sql = string.Format(@"select a.factoryname,a.description,a.elmonthweek,round((a.eltot-nvl(b.elngtot,0))/a.eltot,4)*100||'%' as elrate
                                from (
                                select t2.factoryname,t2.description,t2.elmonthweek,count(t2.historyid) as eltot
                                from (
                                select row_number() over(PARTITION BY t1.factoryname,t1.elmonthweek,t1.historyid ORDER BY t1.txndate DESC) AS rowflag
                                ,t1.factoryname,t1.description,t1.historyid,t1.jkoelresult,t1.elmonthweek
                                from (
                                select f.factoryname,f.description,his.historyid,his.txndate,el.jkoelresult,to_char(his.txndate,'iw')-to_char(last_day(add_months(his.txndate,-1))+1,'iw')+1 as elmonthweek
                                from jkoelmovestdhistory el
                                inner join historymainline his on his.historymainlineid=el.historymainlineid
                                inner join spec s on s.specid=his.specid
                                inner join specbase sb on sb.specbaseid=s.specbaseid
                                inner join factory f on f.factoryid=his.factoryid
                                where sb.specname='EL测试'
                                and his.txndate>=trunc(sysdate,'mm')
                                ) t1
                                ) t2
                                where t2.rowflag=1
                                group by t2.factoryname,t2.description,t2.elmonthweek
                                ) a
                                left join 
                                (select t2.factoryname,t2.description,t2.elmonthweek,count(t2.historyid) as elngtot
                                from (
                                select row_number() over(PARTITION BY t1.factoryname,t1.elmonthweek,t1.historyid ORDER BY t1.txndate DESC) AS rowflag
                                ,t1.factoryname,t1.description,t1.historyid,t1.jkoelresult,t1.elmonthweek
                                from (
                                select f.factoryname,f.description,his.historyid,his.txndate,el.jkoelresult,to_char(his.txndate,'iw')-to_char(last_day(add_months(his.txndate,-1))+1,'iw')+1 as elmonthweek
                                from jkoelmovestdhistory el
                                inner join historymainline his on his.historymainlineid=el.historymainlineid
                                inner join spec s on s.specid=his.specid
                                inner join specbase sb on sb.specbaseid=s.specbaseid
                                inner join factory f on f.factoryid=his.factoryid
                                where sb.specname='EL测试'
                                and el.jkoelresult in ('MC-OK','JK隐裂')
                                and his.txndate>=trunc(sysdate,'mm')
                                ) t1
                                ) t2
                                where t2.rowflag=1
                                group by t2.factoryname,t2.description,t2.elmonthweek)  b on  a.factoryname=b.factoryname and a.elmonthweek=b.elmonthweek");

        DataSet dsMonthWeek = dealDate.GetData(sql, out sErrMsg);
        #endregion

        #region//get el day data
        sql = string.Format(@"select a.factoryname,a.description,a.eldate,round((a.eltot-nvl(b.elngtot,0))/a.eltot,4)*100||'%' as elrate
                                from (
                                select t2.factoryname,t2.description,t2.eldate,count(t2.historyid) as eltot
                                from (
                                select row_number() over(PARTITION BY t1.factoryname,t1.eldate,t1.historyid ORDER BY t1.txndate DESC) AS rowflag
                                ,t1.factoryname,t1.description,t1.historyid,t1.jkoelresult,t1.eldate
                                from (
                                select f.factoryname,f.description,his.historyid,his.txndate,el.jkoelresult,to_char(his.txndate,'YYYY-MM-DD') as eldate
                                from jkoelmovestdhistory el
                                inner join historymainline his on his.historymainlineid=el.historymainlineid
                                inner join spec s on s.specid=his.specid
                                inner join specbase sb on sb.specbaseid=s.specbaseid
                                inner join factory f on f.factoryid=his.factoryid
                                where sb.specname='EL测试'
                                and his.txndate>=to_date(to_char(sysdate-6,'yyyy-mm-dd'),'yyyy-mm-dd')
                                ) t1
                                ) t2
                                where t2.rowflag=1
                                group by t2.factoryname,t2.description,t2.eldate
                                ) a
                                left join 
                                (select t2.factoryname,t2.description,t2.eldate,count(t2.historyid) as elngtot
                                from (
                                select row_number() over(PARTITION BY t1.factoryname,t1.eldate,t1.historyid ORDER BY t1.txndate DESC) AS rowflag
                                ,t1.factoryname,t1.description,t1.historyid,t1.jkoelresult,t1.eldate
                                from (
                                select f.factoryname,f.description,his.historyid,his.txndate,el.jkoelresult,to_char(his.txndate,'YYYY-MM-DD') as eldate
                                from jkoelmovestdhistory el
                                inner join historymainline his on his.historymainlineid=el.historymainlineid
                                inner join spec s on s.specid=his.specid
                                inner join specbase sb on sb.specbaseid=s.specbaseid
                                inner join factory f on f.factoryid=his.factoryid
                                where sb.specname='EL测试'
                                and el.jkoelresult in ('MC-OK','JK隐裂')
                                and his.txndate>=to_date(to_char(sysdate-6,'yyyy-mm-dd'),'yyyy-mm-dd')
                                ) t1
                                ) t2
                                where t2.rowflag=1
                                group by t2.factoryname,t2.description,t2.eldate)  b on  a.factoryname=b.factoryname and a.eldate=b.eldate");

        DataSet dselday = dealDate.GetData(sql, out sErrMsg);
        #endregion

        #region//填充数据
        string sFacotyCode, sFactoryName;
        var vfactory = (from rf in dsQuarter.Tables[0].AsEnumerable()
                        select new
                        {
                            FactoryCode = rf.Field<string>("factoryname"),
                            FactoryName = rf.Field<string>("description")
                        }).Distinct();

        foreach (var r in vfactory)
        {
            sFacotyCode = r.FactoryCode;
            sFactoryName = r.FactoryName;

            DataRow dr = dtReturn.NewRow();
            dr["FACTORY_CODE"] = sFacotyCode;
            dr["FACTORY_NAME"] = sFactoryName;

            #region //wirte quarter data
            for (int i = 1; i <= Convert.ToInt32(elquarter); i++)
            {
                DataRow[] drquarter = dsQuarter.Tables[0].Select("factoryname='" + sFacotyCode + "' and elquarter='" + Convert.ToString(i) + "'");
                if (drquarter.Length > 0)
                {
                    dr["Q" + Convert.ToString(i)] = drquarter[0]["elrate"];
                }
                else
                {
                    dr["Q" + Convert.ToString(i)] = "100%";
                }
            }
            #endregion

            #region//wirte month data
            for (int i = Convert.ToInt32(elqfirstmonth); i <= Convert.ToInt32(elmonth); i++)
            {
                DataRow[] drmonth = dsMonth.Tables[0].Select("factoryname='" + sFacotyCode + "' and elmonth='" + i.ToString("0#") + "'");
                if (drmonth.Length > 0)
                {
                    //dr["M" + Convert.ToString(i)] = drmonth[0]["elrate"];
                    dr[dealDate.GetMonth(Convert.ToString(i))] = drmonth[0]["elrate"];
                }
                else
                {
                    //dr["M" + Convert.ToString(i)] = "100%";
                    dr[dealDate.GetMonth(Convert.ToString(i))] = "100%";
                }
            }
            #endregion

            #region//wirte month week data
            for (int i = 1; i <= Convert.ToInt32(elmonthweek); i++)
            {
                DataRow[] drmonthweek = dsMonthWeek.Tables[0].Select("factoryname='" + sFacotyCode + "' and elmonthweek='" + Convert.ToString(i) + "'");
                if (drmonthweek.Length > 0)
                {
                    //dr["M" + elmonth + "-W" + i.ToString()] = drmonthweek[0]["elrate"];
                    dr[dealDate.GetMonth(elmonth) + "-W" + i.ToString()] = drmonthweek[0]["elrate"];
                }
                else
                {
                    //dr["M" + elmonth + "-W" + i.ToString()] = "100%";
                    dr[dealDate.GetMonth(elmonth) + "-W" + i.ToString()] = "100%";
                }
            }
            #endregion

            #region//wirte day data
            for (int i = 6; i >= 0; i--)
            {
                if (dtnow.AddDays(-i) >= Convert.ToDateTime(elfirstday))
                {
                    DataRow[] drday = dselday.Tables[0].Select("factoryname='" + sFacotyCode + "' and eldate='" + dtnow.AddDays(-i).ToString("yyyy-MM-dd") + "'");
                    if (drday.Length > 0)
                    {
                        //dr["M" + dtnow.AddDays(-i).Month.ToString() + "-D" + dtnow.AddDays(-i).Day.ToString()] = drday[0]["elrate"];
                        dr[dealDate.GetMonth(dtnow.AddDays(-i).Month.ToString()) + "-" + dtnow.AddDays(-i).Day.ToString()] = drday[0]["elrate"];
                    }
                    else
                    {
                        //dr["M" + dtnow.AddDays(-i).Month.ToString() + "-D" + dtnow.AddDays(-i).Day.ToString()] = "100%";
                        dr[dealDate.GetMonth(dtnow.AddDays(-i).Month.ToString()) + "-" + dtnow.AddDays(-i).Day.ToString()] = "100%";
                    }
                }
            }
            #endregion

            dtReturn.Rows.Add(dr);
            dtReturn.AcceptChanges();
        }
        #endregion

        return dtReturn;
    }

    //抽检批次合格率
    [WebMethod]
    public DataTable GetOBAData()
    {
        string sql = string.Empty, sErrMsg = string.Empty;
        string obadate, obayear, obamonth, obaday, obayearweek, obaquarter, obamonthweek, obafirstday, obaqfirstmonth;
        DateTime dtnow;
        DataTable dtReturn = new DataTable();
        dtReturn.TableName = "OBADATA";

        #region//get year month day week quarter
        sql = string.Format(@"select to_char(sysdate,'YYYY-MM-DD') as obadate,to_char(sysdate,'YYYY') as obayear,to_char(sysdate,'MM') as obamonth
                                ,to_char(sysdate,'DD') as obaday,to_char(sysdate,'ww') as obayearweek,to_char(sysdate,'Q') as obaquarter
                                ,to_char(sysdate,'iw')-to_char(last_day(add_months(sysdate,-1))+1,'iw')+1 as obamonthweek,to_char(trunc(sysdate,'y'),'YYYY-MM-DD') as obafirstday
                                ,to_char(to_date(to_char(trunc(sysdate, 'q'), 'yyyy-mm-dd'),'yyyy-mm-dd'),'MM') as obaqfirstmonth
                                from dual");
        DataSet dsdate = dealDate.GetData(sql, out sErrMsg);

        obadate = Convert.ToString(dsdate.Tables[0].Rows[0]["obadate"]);
        obayear = Convert.ToString(dsdate.Tables[0].Rows[0]["obayear"]);
        obamonth = Convert.ToString(dsdate.Tables[0].Rows[0]["obamonth"]);
        obaday = Convert.ToString(dsdate.Tables[0].Rows[0]["obaday"]);
        obayearweek = Convert.ToString(dsdate.Tables[0].Rows[0]["obayearweek"]);
        obaquarter = Convert.ToString(dsdate.Tables[0].Rows[0]["obaquarter"]);
        obamonthweek = Convert.ToString(dsdate.Tables[0].Rows[0]["obamonthweek"]);
        obafirstday = Convert.ToString(dsdate.Tables[0].Rows[0]["obafirstday"]);
        obaqfirstmonth = Convert.ToString(dsdate.Tables[0].Rows[0]["obaqfirstmonth"]);
        dtnow = Convert.ToDateTime(obadate);
        #endregion

        #region//初始化表
        dtReturn.Columns.Add("FACTORY_CODE");
        dtReturn.Columns.Add("FACTORY_NAME");
        for (int i = 1; i <= Convert.ToInt32(obaquarter); i++)
        {
            dtReturn.Columns.Add("Q" + Convert.ToString(i));
        }
        for (int i = Convert.ToInt32(obaqfirstmonth); i <= Convert.ToInt32(obamonth); i++)
        {
            //if (i > 0)
            //{
            //    //dtReturn.Columns.Add("M" + Convert.ToString(i));
            //    dtReturn.Columns.Add(GetMonth(Convert.ToString(i)));
            //}
            dtReturn.Columns.Add(dealDate.GetMonth(Convert.ToString(i)));
        }
        for (int i = 1; i <= Convert.ToInt32(obamonthweek); i++)
        {
            //dtReturn.Columns.Add("M" + obamonth + "-W" + Convert.ToString(i));
            dtReturn.Columns.Add(dealDate.GetMonth(obamonth) + "-W" + Convert.ToString(i));
        }
        for (int i = 6; i >= 0; i--)
        {
            if (dtnow.AddDays(-i) >= Convert.ToDateTime(obafirstday))
            {
                //dtReturn.Columns.Add("M" + dtnow.AddDays(-i).Month.ToString() + "-D" + dtnow.AddDays(-i).Day.ToString());
                dtReturn.Columns.Add(dealDate.GetMonth(dtnow.AddDays(-i).Month.ToString()) + "-" + dtnow.AddDays(-i).Day.ToString());
            }
        }
        #endregion

        #region//get oba quarter data
        sql = string.Format(@"select a.factoryname,a.description,a.obaquarter,round((a.tot-nvl(b.ngtot,0))/a.tot,4)*100||'%' as obarate
                                from (
                                select t1.factoryname,t1.description,t1.obaquarter,count(t1.jkocontainerid) as tot
                                from (
                                select f.factoryname,f.description,obap.jkocontainerid,nvl(obap.jkoinspectionresult,'OK') as jkoinspectionresult
                                ,oba.createordertime,to_char(oba.createordertime,'YYYY-MM-DD') as obadate,to_char(oba.createordertime,'YYYY') as obayear
                                ,to_char(oba.createordertime,'MM') as obamonth,to_char(oba.createordertime,'DD') as obaday,to_char(oba.createordertime,'ww') as obayearweek
                                ,to_char(oba.createordertime,'iw')-to_char(last_day(add_months( oba.createordertime,-1))+1,'iw')+1 as obamonthweek
                                ,to_char(oba.createordertime,'Q') as obaquarter
                                from jkoobaorder oba
                                inner join jkoobaorderdetaillist obal on obal.parentid=oba.jkoobaorderid
                                inner join jkoobapalletdetaillist obap on obap.jkoobaorderid=oba.jkoobaorderid and obap.jkopalletid=obal.jkopalletid
                                inner join factory f on f.factoryid=oba.factoryid
                                where oba.jkoorderstatus='2'
                                and nvl(obap.jkoisoba,0)=1
                                and oba.createordertime>=trunc(sysdate,'y')
                                ) t1
                                group by t1.factoryname,t1.description,t1.obaquarter
                                ) a left join 
                                (
                                select t1.factoryname,t1.description,t1.obaquarter,count(t1.jkocontainerid) as ngtot
                                from (
                                select f.factoryname,f.description,obap.jkocontainerid,nvl(obap.jkoinspectionresult,'OK') as jkoinspectionresult
                                ,oba.createordertime,to_char(oba.createordertime,'YYYY-MM-DD') as obadate,to_char(oba.createordertime,'YYYY') as obayear
                                ,to_char(oba.createordertime,'MM') as obamonth,to_char(oba.createordertime,'DD') as obaday,to_char(oba.createordertime,'ww') as obayearweek
                                ,to_char(oba.createordertime,'iw')-to_char(last_day(add_months( oba.createordertime,-1))+1,'iw')+1 as obamonthweek
                                ,to_char(oba.createordertime,'Q') as obaquarter
                                from jkoobaorder oba
                                inner join jkoobaorderdetaillist obal on obal.parentid=oba.jkoobaorderid
                                inner join jkoobapalletdetaillist obap on obap.jkoobaorderid=oba.jkoobaorderid and obap.jkopalletid=obal.jkopalletid
                                inner join factory f on f.factoryid=oba.factoryid
                                where oba.jkoorderstatus='2'
                                and nvl(obap.jkoisoba,0)=1
                                and obap.jkoinspectionresult='NG'
                                and oba.createordertime>=trunc(sysdate,'y')
                                ) t1
                                group by t1.factoryname,t1.description,t1.obaquarter) b on a.factoryname=b.factoryname and a.obaquarter=b.obaquarter");

        DataSet dsQuarter = dealDate.GetData(sql, out sErrMsg);
        #endregion

        #region//get oba month data
        sql = string.Format(@"select a.factoryname,a.description,a.obamonth,round((a.tot-nvl(b.ngtot,0))/a.tot,4)*100||'%' as obarate
                                from (
                                select t1.factoryname,t1.description,t1.obamonth,count(t1.jkocontainerid) as tot
                                from (
                                select f.factoryname,f.description,obap.jkocontainerid,nvl(obap.jkoinspectionresult,'OK') as jkoinspectionresult
                                ,oba.createordertime,to_char(oba.createordertime,'YYYY-MM-DD') as obadate,to_char(oba.createordertime,'YYYY') as obayear
                                ,to_char(oba.createordertime,'MM') as obamonth,to_char(oba.createordertime,'DD') as obaday,to_char(oba.createordertime,'ww') as obayearweek
                                ,to_char(oba.createordertime,'iw')-to_char(last_day(add_months( oba.createordertime,-1))+1,'iw')+1 as obamonthweek
                                ,to_char(oba.createordertime,'Q') as obaquarter
                                from jkoobaorder oba
                                inner join jkoobaorderdetaillist obal on obal.parentid=oba.jkoobaorderid
                                inner join jkoobapalletdetaillist obap on obap.jkoobaorderid=oba.jkoobaorderid and obap.jkopalletid=obal.jkopalletid
                                inner join factory f on f.factoryid=oba.factoryid
                                where oba.jkoorderstatus='2'
                                and nvl(obap.jkoisoba,0)=1
                                and oba.createordertime>=to_date(to_char(trunc(sysdate, 'q'), 'yyyy-mm-dd'),'yyyy-mm-dd')
                                ) t1
                                group by t1.factoryname,t1.description,t1.obamonth
                                ) a left join 
                                (
                                select t1.factoryname,t1.description,t1.obamonth,count(t1.jkocontainerid) as ngtot
                                from (
                                select f.factoryname,f.description,obap.jkocontainerid,nvl(obap.jkoinspectionresult,'OK') as jkoinspectionresult
                                ,oba.createordertime,to_char(oba.createordertime,'YYYY-MM-DD') as obadate,to_char(oba.createordertime,'YYYY') as obayear
                                ,to_char(oba.createordertime,'MM') as obamonth,to_char(oba.createordertime,'DD') as obaday,to_char(oba.createordertime,'ww') as obayearweek
                                ,to_char(oba.createordertime,'iw')-to_char(last_day(add_months( oba.createordertime,-1))+1,'iw')+1 as obamonthweek
                                ,to_char(oba.createordertime,'Q') as obaquarter
                                from jkoobaorder oba
                                inner join jkoobaorderdetaillist obal on obal.parentid=oba.jkoobaorderid
                                inner join jkoobapalletdetaillist obap on obap.jkoobaorderid=oba.jkoobaorderid and obap.jkopalletid=obal.jkopalletid
                                inner join factory f on f.factoryid=oba.factoryid
                                where oba.jkoorderstatus='2'
                                and nvl(obap.jkoisoba,0)=1
                                and obap.jkoinspectionresult='NG'
                                and oba.createordertime>=to_date(to_char(trunc(sysdate, 'q'), 'yyyy-mm-dd'),'yyyy-mm-dd')
                                ) t1
                                group by t1.factoryname,t1.description,t1.obamonth) b on a.factoryname=b.factoryname and a.obamonth=b.obamonth");

        DataSet dsMonth = dealDate.GetData(sql, out sErrMsg);
        #endregion

        #region//get oba month week data
        sql = string.Format(@"select a.factoryname,a.description,a.obamonthweek,round((a.tot-nvl(b.ngtot,0))/a.tot,4)*100||'%' as obarate
                                from (
                                select t1.factoryname,t1.description,t1.obamonthweek,count(t1.jkocontainerid) as tot
                                from (
                                select f.factoryname,f.description,obap.jkocontainerid,nvl(obap.jkoinspectionresult,'OK') as jkoinspectionresult
                                ,oba.createordertime,to_char(oba.createordertime,'YYYY-MM-DD') as obadate,to_char(oba.createordertime,'YYYY') as obayear
                                ,to_char(oba.createordertime,'MM') as obamonth,to_char(oba.createordertime,'DD') as obaday,to_char(oba.createordertime,'ww') as obayearweek
                                ,to_char(oba.createordertime,'iw')-to_char(last_day(add_months( oba.createordertime,-1))+1,'iw')+1 as obamonthweek
                                ,to_char(oba.createordertime,'Q') as obaquarter
                                from jkoobaorder oba
                                inner join jkoobaorderdetaillist obal on obal.parentid=oba.jkoobaorderid
                                inner join jkoobapalletdetaillist obap on obap.jkoobaorderid=oba.jkoobaorderid and obap.jkopalletid=obal.jkopalletid
                                inner join factory f on f.factoryid=oba.factoryid
                                where oba.jkoorderstatus='2'
                                and nvl(obap.jkoisoba,0)=1
                                and oba.createordertime>=trunc(sysdate,'mm')
                                ) t1
                                group by t1.factoryname,t1.description,t1.obamonthweek
                                ) a left join 
                                (
                                select t1.factoryname,t1.description,t1.obamonthweek,count(t1.jkocontainerid) as ngtot
                                from (
                                select f.factoryname,f.description,obap.jkocontainerid,nvl(obap.jkoinspectionresult,'OK') as jkoinspectionresult
                                ,oba.createordertime,to_char(oba.createordertime,'YYYY-MM-DD') as obadate,to_char(oba.createordertime,'YYYY') as obayear
                                ,to_char(oba.createordertime,'MM') as obamonth,to_char(oba.createordertime,'DD') as obaday,to_char(oba.createordertime,'ww') as obayearweek
                                ,to_char(oba.createordertime,'iw')-to_char(last_day(add_months( oba.createordertime,-1))+1,'iw')+1 as obamonthweek
                                ,to_char(oba.createordertime,'Q') as obaquarter
                                from jkoobaorder oba
                                inner join jkoobaorderdetaillist obal on obal.parentid=oba.jkoobaorderid
                                inner join jkoobapalletdetaillist obap on obap.jkoobaorderid=oba.jkoobaorderid and obap.jkopalletid=obal.jkopalletid
                                inner join factory f on f.factoryid=oba.factoryid
                                where oba.jkoorderstatus='2'
                                and nvl(obap.jkoisoba,0)=1
                                and obap.jkoinspectionresult='NG'
                                and oba.createordertime>=trunc(sysdate,'mm')
                                ) t1
                                group by t1.factoryname,t1.description,t1.obamonthweek) b on a.factoryname=b.factoryname and a.obamonthweek=b.obamonthweek");

        DataSet dsMonthWeek = dealDate.GetData(sql, out sErrMsg);
        #endregion

        #region//get oba day data
        sql = string.Format(@"select a.factoryname,a.description,a.obadate,round((a.tot-nvl(b.ngtot,0))/a.tot,4)*100||'%' as obarate
                                from (
                                select t1.factoryname,t1.description,t1.obadate,count(t1.jkocontainerid) as tot
                                from (
                                select f.factoryname,f.description,obap.jkocontainerid,nvl(obap.jkoinspectionresult,'OK') as jkoinspectionresult
                                ,oba.createordertime,to_char(oba.createordertime,'YYYY-MM-DD') as obadate,to_char(oba.createordertime,'YYYY') as obayear
                                ,to_char(oba.createordertime,'MM') as obamonth,to_char(oba.createordertime,'DD') as obaday,to_char(oba.createordertime,'ww') as obayearweek
                                ,to_char(oba.createordertime,'iw')-to_char(last_day(add_months( oba.createordertime,-1))+1,'iw')+1 as obamonthweek
                                ,to_char(oba.createordertime,'Q') as obaquarter
                                from jkoobaorder oba
                                inner join jkoobaorderdetaillist obal on obal.parentid=oba.jkoobaorderid
                                inner join jkoobapalletdetaillist obap on obap.jkoobaorderid=oba.jkoobaorderid and obap.jkopalletid=obal.jkopalletid
                                inner join factory f on f.factoryid=oba.factoryid
                                where oba.jkoorderstatus='2'
                                and nvl(obap.jkoisoba,0)=1
                                and oba.createordertime>=to_date(to_char(sysdate-6,'yyyy-mm-dd'),'yyyy-mm-dd')
                                ) t1
                                group by t1.factoryname,t1.description,t1.obadate
                                ) a left join 
                                (
                                select t1.factoryname,t1.description,t1.obadate,count(t1.jkocontainerid) as ngtot
                                from (
                                select f.factoryname,f.description,obap.jkocontainerid,nvl(obap.jkoinspectionresult,'OK') as jkoinspectionresult
                                ,oba.createordertime,to_char(oba.createordertime,'YYYY-MM-DD') as obadate,to_char(oba.createordertime,'YYYY') as obayear
                                ,to_char(oba.createordertime,'MM') as obamonth,to_char(oba.createordertime,'DD') as obaday,to_char(oba.createordertime,'ww') as obayearweek
                                ,to_char(oba.createordertime,'iw')-to_char(last_day(add_months( oba.createordertime,-1))+1,'iw')+1 as obamonthweek
                                ,to_char(oba.createordertime,'Q') as obaquarter
                                from jkoobaorder oba
                                inner join jkoobaorderdetaillist obal on obal.parentid=oba.jkoobaorderid
                                inner join jkoobapalletdetaillist obap on obap.jkoobaorderid=oba.jkoobaorderid and obap.jkopalletid=obal.jkopalletid
                                inner join factory f on f.factoryid=oba.factoryid
                                where oba.jkoorderstatus='2'
                                and nvl(obap.jkoisoba,0)=1
                                and obap.jkoinspectionresult='NG'
                                and oba.createordertime>=to_date(to_char(sysdate-6,'yyyy-mm-dd'),'yyyy-mm-dd')
                                ) t1
                                group by t1.factoryname,t1.description,t1.obadate) b on a.factoryname=b.factoryname and a.obadate=b.obadate");

        DataSet dselday = dealDate.GetData(sql, out sErrMsg);
        #endregion

        #region//填充数据
        string sFacotyCode, sFactoryName;
        var vfactory = (from rf in dsMonth.Tables[0].AsEnumerable()
                        select new
                        {
                            FactoryCode = rf.Field<string>("factoryname"),
                            FactoryName = rf.Field<string>("description")
                        }).Distinct();

        foreach (var r in vfactory)
        {
            sFacotyCode = r.FactoryCode;
            sFactoryName = r.FactoryName;

            DataRow dr = dtReturn.NewRow();
            dr["FACTORY_CODE"] = sFacotyCode;
            dr["FACTORY_NAME"] = sFactoryName;

            #region //wirte quarter data
            for (int i = 1; i <= Convert.ToInt32(obaquarter); i++)
            {
                DataRow[] drquarter = dsQuarter.Tables[0].Select("factoryname='" + sFacotyCode + "' and obaquarter='" + Convert.ToString(i) + "'");
                if (drquarter.Length > 0)
                {
                    dr["Q" + Convert.ToString(i)] = drquarter[0]["obarate"];
                }
                else
                {
                    dr["Q" + Convert.ToString(i)] = "100%";
                }
            }
            #endregion

            #region//wirte month data
            for (int i = Convert.ToInt32(obaqfirstmonth); i <= Convert.ToInt32(obamonth); i++)
            {
                if (i > 0)
                {
                    DataRow[] drmonth = dsMonth.Tables[0].Select("factoryname='" + sFacotyCode + "' and obamonth='" + i.ToString("0#") + "'");
                    if (drmonth.Length > 0)
                    {
                        //dr["M" + Convert.ToString(i)] = drmonth[0]["obarate"];
                        dr[dealDate.GetMonth(Convert.ToString(i))] = drmonth[0]["obarate"];
                    }
                    else
                    {
                        //dr["M" + Convert.ToString(i)] = "100%";
                        dr[dealDate.GetMonth(Convert.ToString(i))] = "100%";
                    }
                }
            }
            #endregion

            #region//wirte month week data
            for (int i = 1; i <= Convert.ToInt32(obamonthweek); i++)
            {
                DataRow[] drmonthweek = dsMonthWeek.Tables[0].Select("factoryname='" + sFacotyCode + "' and obamonthweek='" + Convert.ToString(i) + "'");
                if (drmonthweek.Length > 0)
                {
                    //dr["M" + obamonth + "-W" + i.ToString()] = drmonthweek[0]["obarate"];
                    dr[dealDate.GetMonth(obamonth) + "-W" + i.ToString()] = drmonthweek[0]["obarate"];
                }
                else
                {
                    //dr["M" + obamonth + "-W" + i.ToString()] = "100%";
                    dr[dealDate.GetMonth(obamonth) + "-W" + i.ToString()] = "100%";
                }
            }
            #endregion

            #region//wirte day data
            for (int i = 6; i >= 0; i--)
            {
                if (dtnow.AddDays(-i) >= Convert.ToDateTime(obafirstday))
                {
                    DataRow[] drday = dselday.Tables[0].Select("factoryname='" + sFacotyCode + "' and obadate='" + dtnow.AddDays(-i).ToString("yyyy-MM-dd") + "'");
                    if (drday.Length > 0)
                    {
                        //dr["M" + dtnow.AddDays(-i).Month.ToString() + "-D" + dtnow.AddDays(-i).Day.ToString()] = drday[0]["obarate"];
                        dr[dealDate.GetMonth(dtnow.AddDays(-i).Month.ToString()) + "-" + dtnow.AddDays(-i).Day.ToString()] = drday[0]["obarate"];
                    }
                    else
                    {
                        //dr["M" + dtnow.AddDays(-i).Month.ToString() + "-D" + dtnow.AddDays(-i).Day.ToString()] = "100%";
                        dr[dealDate.
GetMonth(dtnow.AddDays(-i).Month.ToString()) + "-" + dtnow.AddDays(-i).Day.ToString()] = "100%";
                    }
                }
            }
            #endregion

            dtReturn.Rows.Add(dr);
            dtReturn.AcceptChanges();
        }
        #endregion

        return dtReturn;
    }

    //破片率（中控室）
    [WebMethod]
    public DataSet GetODData()
    {
        DataSet dsResult = new DataSet();
        string msg = string.Empty;

        //设置语言为英文;
        using (OracleConnection conn = new OracleConnection())
        {
            try
            {
                conn.ConnectionString = ConfigurationManager.ConnectionStrings["MES"].ConnectionString;
                conn.Open();
                OracleCommand comm = conn.CreateCommand();
                comm.CommandText = "ALTER SESSION SET NLS_DATE_LANGUAGE = 'american'";
                comm.CommandType = CommandType.Text;
                comm.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                msg = ex.Message;
                throw ex;
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        //string sql = string.Format(@"SELECT * FROM JKO_FRAGMENTATION_RATE WHERE TXNDATE BETWEEN TO_CHAR(sysdate - 14,'yyyy-mm-dd') AND TO_CHAR(sysdate,'yyyy-mm-dd')");
        string sql = string.Format(@"WITH GS AS(
SELECT DISTINCT 1 RN,
TO_CHAR(HM.TXNDATE,'yyyy-mm-dd')||DCP.TOCONTAINERID||S.SHIFTNAME TAG,
TO_CHAR(HM.TXNDATE,'yyyy-mm-dd') TXNDATE,
F.FACTORYNAME,
F.DESCRIPTION,
HM.TXNID,
DCP.TOCONTAINERID,
IHD.ACTUALQTYISSUED QTY,
S.SHIFTNAME
FROM (SELECT IAH.JKOCELLSTRING,IAH.ISSUEHISTORYDETAILID,IAH.FROMCONTAINERID,IAH.TOCONTAINERID 
FROM ISSUEACTUALSHISTORY IAH 
INNER JOIN PRODUCT P ON IAH.PRODUCTID = P.PRODUCTID 
INNER JOIN PRODUCTTYPE PT ON P.PRODUCTTYPEID = PT.PRODUCTTYPEID AND PT.DESCRIPTION = '电池片') DCP 
INNER JOIN ISSUEHISTORYDETAIL IHD ON IHD.ISSUEHISTORYDETAILID = DCP.ISSUEHISTORYDETAILID 
INNER JOIN HISTORYMAINLINE HM ON IHD.TXNID = HM.TXNID
INNER JOIN CONTAINER TC ON TC.CONTAINERID = DCP.TOCONTAINERID --组件
--班次--
INNER JOIN FACTORY F ON TC.ORIGINALFACTORYID = F.FACTORYID 
INNER JOIN MFGCALENDAR MC ON MC.MFGCALENDARID=F.MFGCALENDARID
INNER JOIN CALENDARSHIFT CF ON CF.MFGCALENDARID=MC.MFGCALENDARID 
INNER JOIN SHIFT S ON S.SHIFTID=CF.SHIFTID
INNER JOIN SPEC S ON HM.SPECID = S.SPECID 
INNER JOIN SPECBASE SB ON S.SPECBASEID = SB.SPECBASEID 
AND S.SPECID = REPLACE(HM.SPECID,'0000000000000000',SB.REVOFRCDID)
WHERE HM.TXNTYPENAME = 'jkoCellReplaceByResource' 
AND HM.SPECID IS NOT NULL AND SB.SPECNAME = '串焊' 
AND HM.TXNDATE BETWEEN CF.SHIFTSTART AND CF.SHIFTEND
),
GYS AS
(
SELECT 
X.RN
,TO_CHAR(X.TXNDATE,'yyyy-mm-dd') TXNDATE
,TO_CHAR(X.TXNDATE,'yyyy-mm-dd')||X.JKORESERVEMFGORDERID||S.SHIFTNAME TAG
,F.FACTORYNAME
,F.DESCRIPTION
,X.TXNID
,X.HISTORYID
,X.JKOISCELLORIG
,X.QTY
,X.JKORESERVEMFGORDERID
,S.SHIFTNAME FROM (
(SELECT
ROW_NUMBER() OVER( PARTITION BY HM.TXNID ORDER BY HM.TXNDATE DESC) AS RN,
HM.TXNDATE,
C.ORIGINALFACTORYID,
HM.TXNID,
B.HISTORYID,
C.JKORESERVEMFGORDERID,
NVL(B.JKOISCELLORIG,0) JKOISCELLORIG,
B.QTY 
 FROM CONTAINER C 
INNER JOIN SPLITHISTORYDETAILS B ON C.CONTAINERID=B.TOCONTAINERID 
INNER JOIN HISTORYMAINLINE HM ON B.TXNID = HM.TXNID AND B.HISTORYID = HM.HISTORYID 
INNER JOIN PRODUCT P ON C.PRODUCTID = P.PRODUCTID 
INNER JOIN PRODUCTTYPE PT ON P.PRODUCTTYPEID = PT.PRODUCTTYPEID
INNER JOIN PRODUCTBASE PB ON P.PRODUCTBASEID = PB.PRODUCTBASEID
WHERE NVL(C.JKOISMATERIALDEFECT,0)=1 AND PT.DESCRIPTION = '电池片')
UNION ALL 
(SELECT DISTINCT
ROW_NUMBER() OVER( PARTITION BY HM.TXNID ORDER BY HM.TXNDATE DESC) AS RN,
HM.TXNDATE,
C.ORIGINALFACTORYID,
HM.TXNID,
SPL.HISTORYID,
C.JKORESERVEMFGORDERID,
NVL(SPL.JKOISCELLORIG,0) JKOISCELLORIG,
CMB.QTY
 FROM CONTAINER C 
INNER JOIN COMBINEHISTORY CMB ON C.CONTAINERID=CMB.CONTAINERID 
INNER JOIN SPLITHISTORYDETAILS SPL ON CMB.CONTAINERID = SPL.TOCONTAINERID
INNER JOIN HISTORYMAINLINE HM ON CMB.historymainlineid = HM.historymainlineid
INNER JOIN PRODUCT P ON C.PRODUCTID = P.PRODUCTID 
INNER JOIN PRODUCTTYPE PT ON P.PRODUCTTYPEID = PT.PRODUCTTYPEID
INNER JOIN PRODUCTBASE PB ON P.PRODUCTBASEID = PB.PRODUCTBASEID
WHERE NVL(C.JKOISMATERIALDEFECT,0)=1 AND PT.DESCRIPTION = '电池片')
) X 
--班次--
INNER JOIN FACTORY F ON X.ORIGINALFACTORYID = F.FACTORYID 
INNER JOIN MFGCALENDAR MC ON MC.MFGCALENDARID=F.MFGCALENDARID
INNER JOIN CALENDARSHIFT CF ON CF.MFGCALENDARID=MC.MFGCALENDARID 
INNER JOIN SHIFT S ON S.SHIFTID=CF.SHIFTID 
WHERE X.RN = '1' AND X.TXNDATE BETWEEN CF.SHIFTSTART AND CF.SHIFTEND
),
DTT AS(
SELECT 
ROW_NUMBER() OVER(PARTITION BY HM.CONTAINERID ORDER BY HM.TXNID DESC) AS RN,
TO_CHAR(HM.TXNDATE,'yyyy-mm-dd')||M.MFGORDERID||S.SHIFTNAME TAG,
HM.CONTAINERID,
F.FACTORYNAME,
F.DESCRIPTION,
HM.TXNID,
TO_CHAR(HM.TXNDATE,'yyyy-mm-dd') TXNDATE,
M.MFGORDERID,
P.JKOATTRIBUTE12DESCRIPTION||P.JKOATTRIBUTE11DESCRIPTION DESCP, 
B.QTY,
NVL(GS.QTY,0) GSQTY,
S.SHIFTNAME
FROM HISTORYMAINLINE 
HM INNER JOIN SPEC S ON HM.SPECID = S.SPECID 
INNER JOIN SPECBASE SB ON S.SPECBASEID = SB.SPECBASEID AND SB.SPECNAME = '串焊' 
INNER JOIN (SELECT A.CONTAINERID,SUM(A.QTY) QTY FROM JKOCONTAINERCELLSTRINGS A 
GROUP BY A.CONTAINERID) B ON HM.CONTAINERID=B.CONTAINERID
INNER JOIN CONTAINER ZJ ON HM.CONTAINERID = ZJ.CONTAINERID
INNER JOIN PRODUCT P ON ZJ.PRODUCTID = P.PRODUCTID 
INNER JOIN MFGORDER M ON M.MFGORDERID = ZJ.MFGORDERID
INNER JOIN FACTORY F ON ZJ.ORIGINALFACTORYID = F.FACTORYID 
INNER JOIN MFGCALENDAR MC ON MC.MFGCALENDARID=F.MFGCALENDARID
INNER JOIN CALENDARSHIFT CF ON CF.MFGCALENDARID=MC.MFGCALENDARID 
INNER JOIN SHIFT S ON S.SHIFTID=CF.SHIFTID 
LEFT JOIN GS ON TO_CHAR(HM.TXNDATE,'yyyy-mm-dd')||ZJ.CONTAINERID||S.SHIFTNAME = GS.TAG
WHERE HM.TXNTYPENAME = 'MoveStd' 
AND TO_CHAR(HM.TXNDATE,'yyyy-mm-dd') BETWEEN TO_CHAR(sysdate - 14,'yyyy-mm-dd') AND TO_CHAR(sysdate,'yyyy-mm-dd')
AND HM.TXNDATE BETWEEN CF.SHIFTSTART AND CF.SHIFTEND
),
DT AS(
SELECT 
ROW_NUMBER() OVER(PARTITION BY DTT.TXNDATE,DTT.MFGORDERID ORDER BY DTT.TXNID ASC) AS RR,
DTT.* FROM DTT WHERE DTT.RN = '1'),
DTSOURCE AS(
SELECT 
TO_DATE(DT.TXNDATE,'yyyy-mm-dd') TXNDATE
,DT.DESCP
,DT.FACTORYNAME
,DT.DESCRIPTION
,SUM(DT.QTY) SMY
,DT.SHIFTNAME
,SUM(DT.GSQTY) + SUM(NVL(GS.QTY,0)) GSQTY
,SUM(NVL(YS.QTY,0)) YSQTY
FROM DT 
LEFT JOIN GYS GS 
ON DT.TAG = GS.TAG AND GS.JKOISCELLORIG = '1' AND DT.RR = '1' --工损
LEFT JOIN GYS YS 
ON DT.TAG = YS.TAG AND YS.JKOISCELLORIG = '0' AND DT.RR = '1' --原损
GROUP BY DT.TXNDATE,DT.DESCP,DT.SHIFTNAME,DT.FACTORYNAME,DT.DESCRIPTION
)
SELECT 
INITCAP(TO_CHAR(DTSOURCE.TXNDATE,'mon'))||'-D'||TO_CHAR(DTSOURCE.TXNDATE,'dd') TXNDATE
,DTSOURCE.DESCP
,TO_CHAR(0,'fm9999999990.00')||'%' GSTARGET
,TO_CHAR(0,'fm9999999990.00')||'%' YSTARGET
,TO_CHAR(ROUND(SUM(DTSOURCE.GSQTY)*100/SUM(DTSOURCE.SMY),2),'fm9999999990.00')||'%' GSRATE
,TO_CHAR(ROUND(SUM(DTSOURCE.YSQTY)*100/SUM(DTSOURCE.SMY),2),'fm9999999990.00')||'%' RSRATE 
,DTSOURCE.FACTORYNAME
,DTSOURCE.DESCRIPTION
FROM DTSOURCE 
GROUP BY 
DTSOURCE.TXNDATE
,DTSOURCE.DESCP
,DTSOURCE.FACTORYNAME
,DTSOURCE.DESCRIPTION 
ORDER BY DTSOURCE.TXNDATE ASC");
        try
        {
            dsResult = dealDate.
GetData(sql, out msg);
        }
        catch (Exception ex)
        {
            msg = ex.Message;
        }
        return dsResult;
    }

    //所有车间最近24小时的产能（中控室）
    [WebMethod]
    public DataSet GetCapacityList()
    {
        DataSet dsResult = new DataSet();
        #region

        DateTime dtime = DateTime.Now; //new DateTime(2018, 04, 18,01,30,00);//测试用数据//

        int iHour = dtime.Hour;

        string strtime = dtime.ToString("yyyy-MM-dd");
        string strtime2 = dtime.AddDays(-1).ToString("yyyy-MM-dd");

        string msg = string.Empty;
        bool subDay = true;

        #region sql
        string sqlString = string.Format(@"
select * from (
select * from (select M1.factoryname,M1.description,M1.datas,M1.times,COUNT(M1.historyid) qty
from (
select row_number() over(PARTITION BY fa.factoryid,sb.specname,hml.historyid ORDER BY hml.txndate asc) AS rowflag,
fa.factoryname,fa.description,sb.specname,hml.historyid,to_char(hml.txndate,'yyyy-mm-dd') datas
,(case when to_char(hml.txndate,'hh24') in ('00','01') then '02:00'
       when to_char(hml.txndate,'hh24') in ('02','03') then '04:00'
       when to_char(hml.txndate,'hh24') in ('04','05') then '06:00'
       when to_char(hml.txndate,'hh24') in ('06','07') then '08:00'
       when to_char(hml.txndate,'hh24') in ('08','09') then '10:00'
       when to_char(hml.txndate,'hh24') in ('10','11') then '12:00'      
       when to_char(hml.txndate,'hh24') in ('12','13') then '14:00'
       when to_char(hml.txndate,'hh24') in ('14','15') then '16:00'
       when to_char(hml.txndate,'hh24') in ('16','17') then '18:00'
       when to_char(hml.txndate,'hh24') in ('18','19') then '20:00'
       when to_char(hml.txndate,'hh24') in ('20','21') then '22:00'
       when to_char(hml.txndate,'hh24') in ('22','23') then '24:00'
  end) times
from historymainline hml
inner join spec s on hml.specid=s.specid
inner join specbase sb on s.specbaseid=sb.specbaseid and s.specid=replace(s.specid,'0000000000000000',sb.revofrcdid)
inner join factory fa on fa.factoryid=hml.factoryid
where 1=1 
--and  sb.specname='串焊' 
and hml.txntypename='MoveStd'

and to_char(hml.txndate,'YYYY-MM-DD')='{0}'
) M1 WHERE M1.rowflag=1

GROUP BY M1.factoryname,M1.description,M1.datas,M1.times
--order by M1.factoryname,M1.datas,M1.times
) temp1

union 

select * from  (select M1.factoryname,M1.description,M1.datas,M1.times,COUNT(M1.historyid) qty
from (
select row_number() over(PARTITION BY fa.factoryid,sb.specname,hml.historyid ORDER BY hml.txndate asc) AS rowflag,
fa.factoryname,fa.description,sb.specname,hml.historyid,to_char(hml.txndate,'yyyy-mm-dd') datas
,(case when to_char(hml.txndate,'hh24') in ('00','01') then '02:00'
       when to_char(hml.txndate,'hh24') in ('02','03') then '04:00'
       when to_char(hml.txndate,'hh24') in ('04','05') then '06:00'
       when to_char(hml.txndate,'hh24') in ('06','07') then '08:00'
       when to_char(hml.txndate,'hh24') in ('08','09') then '10:00'
       when to_char(hml.txndate,'hh24') in ('10','11') then '12:00'      
       when to_char(hml.txndate,'hh24') in ('12','13') then '14:00'
       when to_char(hml.txndate,'hh24') in ('14','15') then '16:00'
       when to_char(hml.txndate,'hh24') in ('16','17') then '18:00'
       when to_char(hml.txndate,'hh24') in ('18','19') then '20:00'
       when to_char(hml.txndate,'hh24') in ('20','21') then '22:00'
       when to_char(hml.txndate,'hh24') in ('22','23') then '24:00'
  end) times
from historymainline hml
inner join spec s on hml.specid=s.specid
inner join specbase sb on s.specbaseid=sb.specbaseid and s.specid=replace(s.specid,'0000000000000000',sb.revofrcdid)
inner join factory fa on fa.factoryid=hml.factoryid
where 1=1 
--and sb.specname='串焊'  
and hml.txntypename='MoveStd'
and to_char(hml.txndate,'YYYY-MM-DD')='{1}'
) M1 WHERE M1.rowflag=1

GROUP BY M1.factoryname,M1.description,M1.datas,M1.times
--order by M1.factoryname,M1.datas,M1.times
) temp2
)
order by factoryname,description,datas,times
", strtime, strtime2);

        string sqlString1 = string.Format(@"

select * from (
select * from (select M1.factoryname,M1.description,M1.datas,M1.times,COUNT(M1.historyid) qty
from (
select row_number() over(PARTITION BY fa.factoryid,sb.specname,hml.historyid ORDER BY hml.txndate asc) AS rowflag,
fa.factoryname,fa.description,sb.specname,hml.historyid,to_char(hml.txndate,'yyyy-mm-dd') datas
,(case when to_char(hml.txndate,'hh24') in ('00','01') then '02:00'
       when to_char(hml.txndate,'hh24') in ('02','03') then '04:00'
       when to_char(hml.txndate,'hh24') in ('04','05') then '06:00'
       when to_char(hml.txndate,'hh24') in ('06','07') then '08:00'
       when to_char(hml.txndate,'hh24') in ('08','09') then '10:00'
       when to_char(hml.txndate,'hh24') in ('10','11') then '12:00'      
       when to_char(hml.txndate,'hh24') in ('12','13') then '14:00'
       when to_char(hml.txndate,'hh24') in ('14','15') then '16:00'
       when to_char(hml.txndate,'hh24') in ('16','17') then '18:00'
       when to_char(hml.txndate,'hh24') in ('18','19') then '20:00'
       when to_char(hml.txndate,'hh24') in ('20','21') then '22:00'
       when to_char(hml.txndate,'hh24') in ('22','23') then '24:00'
  end) times
from historymainline hml
inner join spec s on hml.specid=s.specid
inner join specbase sb on s.specbaseid=sb.specbaseid and s.specid=replace(s.specid,'0000000000000000',sb.revofrcdid)
inner join factory fa on fa.factoryid=hml.factoryid
where 1=1 
--and  sb.specname='串焊'
and sb.specname='FI测试' 
and hml.txntypename='MoveStd'

and to_char(hml.txndate,'YYYY-MM-DD')='{0}'
) M1 WHERE M1.rowflag=1

GROUP BY M1.factoryname,M1.description,M1.datas,M1.times
--order by M1.factoryname,M1.datas,M1.times
) temp1

union 

select * from  (select M1.factoryname,M1.description,M1.datas,M1.times,COUNT(M1.historyid) qty
from (
select row_number() over(PARTITION BY fa.factoryid,sb.specname,hml.historyid ORDER BY hml.txndate asc) AS rowflag,
fa.factoryname,fa.description,sb.specname,hml.historyid,to_char(hml.txndate,'yyyy-mm-dd') datas
,(case when to_char(hml.txndate,'hh24') in ('00','01') then '02:00'
       when to_char(hml.txndate,'hh24') in ('02','03') then '04:00'
       when to_char(hml.txndate,'hh24') in ('04','05') then '06:00'
       when to_char(hml.txndate,'hh24') in ('06','07') then '08:00'
       when to_char(hml.txndate,'hh24') in ('08','09') then '10:00'
       when to_char(hml.txndate,'hh24') in ('10','11') then '12:00'      
       when to_char(hml.txndate,'hh24') in ('12','13') then '14:00'
       when to_char(hml.txndate,'hh24') in ('14','15') then '16:00'
       when to_char(hml.txndate,'hh24') in ('16','17') then '18:00'
       when to_char(hml.txndate,'hh24') in ('18','19') then '20:00'
       when to_char(hml.txndate,'hh24') in ('20','21') then '22:00'
       when to_char(hml.txndate,'hh24') in ('22','23') then '24:00'
  end) times
from historymainline hml
inner join spec s on hml.specid=s.specid
inner join specbase sb on s.specbaseid=sb.specbaseid and s.specid=replace(s.specid,'0000000000000000',sb.revofrcdid)
inner join factory fa on fa.factoryid=hml.factoryid
where 1=1 
--and  sb.specname='串焊'
and sb.specname='FI测试'  
and hml.txntypename='MoveStd'
and to_char(hml.txndate,'YYYY-MM-DD')='{1}'
) M1 WHERE M1.rowflag=1

GROUP BY M1.factoryname,M1.description,M1.datas,M1.times
--order by M1.factoryname,M1.datas,M1.times
) temp2
)
order by factoryname,description,datas,times
", strtime, strtime2);
        #endregion

        try
        {
            DataSet ds = dealDate.GetData(sqlString1, out msg);
            dsResult = ds.Clone();
            dsResult.Clear();
            string strHour = string.Empty;
            int num = 0;

            if (ds != null && ds.Tables[0].Rows.Count > 0)
            {
                while (num < 12)
                {
                    //旧方法
                    //if (iHour % 2 != 0)
                    //{
                    //    strHour = (iHour + 1).ToString();
                    //    iHour = iHour - 1;
                    //}
                    //else
                    //{
                    //    strHour = (iHour + 2).ToString();
                    //    iHour = iHour - 2;
                    //}

                    //if (strHour.Length < 2)
                    //{
                    //    strHour = "0" + strHour;
                    //}
                    //strHour = strHour + ":00";

                    //if (iHour == -4)
                    //{
                    //    iHour = 20;
                    //    strtime = dtime.AddDays(-1).ToString("yyyy-MM-dd");
                    //}

                    switch (dtime.Hour)
                    {
                        case 00:
                            strHour = "02:00";
                            break;
                        case 01:
                            strHour = "02:00";
                            break;
                        case 02:
                            strHour = "04:00";
                            break;
                        case 03:
                            strHour = "04:00";
                            break;
                        case 04:
                            strHour = "06:00";
                            break;
                        case 05:
                            strHour = "06:00";
                            break;
                        case 06:
                            strHour = "08:00";
                            break;
                        case 07:
                            strHour = "08:00";
                            break;
                        case 08:
                            strHour = "10:00";
                            break;
                        case 09:
                            strHour = "10:00";
                            break;
                        case 10:
                            strHour = "12:00";
                            break;
                        case 11:
                            strHour = "12:00";
                            break;
                        case 12:
                            strHour = "14:00";
                            break;
                        case 13:
                            strHour = "14:00";
                            break;
                        case 14:
                            strHour = "16:00";
                            break;
                        case 15:
                            strHour = "16:00";
                            break;
                        case 16:
                            strHour = "18:00";
                            break;
                        case 17:
                            strHour = "18:00";
                            break;
                        case 18:
                            strHour = "20:00";
                            break;
                        case 19:
                            strHour = "20:00";
                            break;
                        case 20:
                            strHour = "22:00";
                            break;
                        case 21:
                            strHour = "22:00";
                            break;
                        case 22:
                            strHour = "24:00";
                            break;
                        case 23:
                            strHour = "24:00";
                            break;
                    }

                    DateTime time12 = Convert.ToDateTime("00:00");
                    if (DateTime.Compare(dtime, time12) <= 0 && subDay)
                    {
                        subDay = false;
                        dtime = dtime.AddDays(-1);
                        dtime = dtime.AddHours(22);
                        strtime = dtime.ToString("yyyy-MM-dd");
                    }
                    else
                    {
                        dtime = dtime.AddHours(-2);
                    }

                    DataRow[] drs = ds.Tables[0].Select("datas='" + strtime + "' and  times = '" + strHour + "'");
                    if (drs.Length > 0)
                    {
                        List<string> needAddData = new List<string>();
                        needAddData.Add("MS0021");
                        needAddData.Add("MS0022");
                        needAddData.Add("MSF002");
                        foreach (var item in drs)
                        {
                            dsResult.Tables[0].ImportRow(item);
                            needAddData.Remove(item["FACTORYNAME"].ToString());
                        }
                        foreach (var oneData in needAddData)
                        {
                            if (oneData == "MS0021")
                            {
                                DataRow oneRow = dsResult.Tables[0].NewRow();
                                oneRow["FACTORYNAME"] = "MS0021";
                                oneRow["DESCRIPTION"] = "上饶五厂组件一车间";
                                oneRow["DATAS"] = strtime;
                                oneRow["TIMES"] = strHour;
                                oneRow["QTY"] = "0";
                                dsResult.Tables[0].Rows.Add(oneRow);
                            }
                            else if (oneData == "MS0022")
                            {
                                DataRow oneRow1 = dsResult.Tables[0].NewRow();
                                oneRow1["FACTORYNAME"] = "MS0022";
                                oneRow1["DESCRIPTION"] = "上饶五厂组件一车间B区";
                                oneRow1["DATAS"] = strtime;
                                oneRow1["TIMES"] = strHour;
                                oneRow1["QTY"] = "0";
                                dsResult.Tables[0].Rows.Add(oneRow1);
                            }
                            else
                            {
                                DataRow oneRow2 = dsResult.Tables[0].NewRow();
                                oneRow2["FACTORYNAME"] = "MSF002";
                                oneRow2["DESCRIPTION"] = "五厂返工组";
                                oneRow2["DATAS"] = strtime;
                                oneRow2["TIMES"] = strHour;
                                oneRow2["QTY"] = "0";
                                dsResult.Tables[0].Rows.Add(oneRow2);
                            }
                        }
                    }
                    else
                    {
                        DataRow oneRow = dsResult.Tables[0].NewRow();
                        oneRow["FACTORYNAME"] = "MS0021";
                        oneRow["DESCRIPTION"] = "上饶五厂组件一车间";
                        oneRow["DATAS"] = strtime;
                        oneRow["TIMES"] = strHour;
                        oneRow["QTY"] = "0";
                        dsResult.Tables[0].Rows.Add(oneRow);
                        DataRow oneRow1 = dsResult.Tables[0].NewRow();
                        oneRow1["FACTORYNAME"] = "MS0022";
                        oneRow1["DESCRIPTION"] = "上饶五厂组件一车间B区";
                        oneRow1["DATAS"] = strtime;
                        oneRow1["TIMES"] = strHour;
                        oneRow1["QTY"] = "0";
                        dsResult.Tables[0].Rows.Add(oneRow1);
                        DataRow oneRow2 = dsResult.Tables[0].NewRow();
                        oneRow2["FACTORYNAME"] = "MSF002";
                        oneRow2["DESCRIPTION"] = "五厂返工组";
                        oneRow2["DATAS"] = strtime;
                        oneRow2["TIMES"] = strHour;
                        oneRow2["QTY"] = "0";
                        dsResult.Tables[0].Rows.Add(oneRow2);
                    }
                    num++;
                }
            }
        }
        catch (Exception ex)
        {
            msg = ex.Message;
        }
        #endregion

        return dsResult;
    }

    //工单完成进度（中控室）
    [WebMethod]
    public DataSet GetMoCompletedProgressList()
    {

        DataSet dsResult = new DataSet();
        string msg = string.Empty;
        //sum(nvl(t.fx,'0')) ""分选"",
        string sqlString = string.Format(@"select t.factoryname,t.description,t.salesordername,t.mfgordername,t.moqty,t.dates,
sum(nvl(t.ch, '0')) ""串焊"",
sum(nvl(t.cd, '0')) ""层叠"",
sum(nvl(t.cy, '0')) ""层压"",
sum(nvl(t.zk, '0')) ""装框"",
sum(nvl(t.qx, '0')) ""清洗"",
sum(nvl(t.ivcs, '0')) + sum(nvl(t.elcs, '0')) ""IV/EL"",
sum(nvl(t.fics, '0')) ""FI"",
sum(nvl(t.bz, '0')) ""包装"",
sum(nvl(t.rk, '0')) ""入库""
from(
select * from(
select
        f.factoryname
       , f.description
        , so.salesordername
       , mo.mfgordername
       , mo.qty moqty
       , c.qty
       , c.containerid
       , upper(sb.specbaseid) specbaseid
       , to_char(mo.plannedstartdate, 'yyyy-mm-dd hh24:Mi:ss') dates
    from mfgorder mo
   inner
    join jkocir cir on mo.jkocirid = cir.jkocirid

inner
    join salesorder so on so.salesorderid = cir.salesorderid

inner
    join container c on c.mfgorderid = mo.mfgorderid

inner
    join currentstatus cs on cs.currentstatusid = c.currentstatusid

inner
    join spec spec on spec.specid = cs.specid

inner
    join specbase sb on spec.specbaseid = sb.specbaseid

left
    join factory f on f.factoryid = cs.factoryid
   ) temp
    pivot(sum(temp.qty) for specbaseid in(
  '0007128000000002' fx, '0007128000000003' ch, '0007128000000004' cd, '0007128000000005' jjel, '0007128000000006' cy,
'0007128000000007' fzj, '0007128000000008' zk, '0007128000000009' gh, '000712800000000a' qx, '000712800000000b' agcs,
'000712800000000c' ivcs, '000712800000000d' elcs, '000712800000000e' fics, '000712800000000f' bz, '0007128000000010' bcpc,
'0007128000000011' cpc, '0007128000000012' ylc, '0007128000000013' cqwx, '0007128000000014' chfx, '0007128000000015' oba,
'00071280000000b1' bzcs, '00071280000000b3' u96ict, '0007128000000192' cq, '0007128000000481' rk, '0007128000000482' rksh,
'0007128000000516' cp1, '0007128000000517' cp2, '0007128000000518' cp3, '0007128000000519' wl1, '000712800000051a' wl2,
'0007128000000642' agjyny, '0007128000000643' agjddz, '4800b38000000002' ipqaspec1, '4800b38000000034' ipqaspec2
    ))  
    order by mfgordername asc
    ) t
    left join
(
select c.containername, qd.containerid, sum(qd.qty) qty from qtyhistorydetails qd, container c
where c.containerid = qd.containerid and qd.changeqtytype = '2' group by c.containername, qd.containerid
 ) b
 on t.containerid = b.containerid
    where  {0}

group by t.factoryname,t.description, t.salesordername,t.mfgordername,t.moqty,t.dates"
, " 1 = 1");
        string sqlString1 = string.Format(@"
select t.factoryname,t.description,t.salesordername,t.mfgordername,t.moqty,t.dates,t.orderstatusname,
sum(nvl(t.ch, '0')) ""串焊"",
sum(nvl(t.cd, '0')) ""层叠"",
sum(nvl(t.cy, '0')) ""层压"",
sum(nvl(t.zk, '0')) ""装框"",
sum(nvl(t.qx, '0')) ""清洗"",
sum(nvl(t.ivcs, '0')) + sum(nvl(t.elcs, '0')) ""IV/EL"",
sum(nvl(t.fics, '0')) ""FI"",
sum(nvl(t.bz, '0')) ""包装"",
sum(nvl(t.rk, '0')) ""入库""
from(
select * from(
select
        f.factoryname
       , f.description
        , so.salesordername
       , mo.mfgordername
       , mo.qty moqty
       , c.qty
       , c.containerid
       , upper(sb.specbaseid) specbaseid
       , to_char(mo.plannedstartdate, 'yyyy-mm-dd hh24:Mi:ss') dates
       , os.orderstatusname
    from mfgorder mo
   inner
    join jkocir cir on mo.jkocirid = cir.jkocirid

inner
    join salesorder so on so.salesorderid = cir.salesorderid

inner
    join container c on c.mfgorderid = mo.mfgorderid

inner
    join currentstatus cs on cs.currentstatusid = c.currentstatusid

inner
    join spec spec on spec.specid = cs.specid

inner
    join specbase sb on spec.specbaseid = sb.specbaseid

left
    join factory f on f.factoryid = cs.factoryid
inner
    join orderstatus os

  on mo.orderstatusid = os.orderstatusid--工单状态
   ) temp
    pivot(sum(temp.qty) for specbaseid in(
  '0007128000000002' fx, '0007128000000003' ch, '0007128000000004' cd, '0007128000000005' jjel, '0007128000000006' cy,
'0007128000000007' fzj, '0007128000000008' zk, '0007128000000009' gh, '000712800000000a' qx, '000712800000000b' agcs,
'000712800000000c' ivcs, '000712800000000d' elcs, '000712800000000e' fics, '000712800000000f' bz, '0007128000000010' bcpc,
'0007128000000011' cpc, '0007128000000012' ylc, '0007128000000013' cqwx, '0007128000000014' chfx, '0007128000000015' oba,
'00071280000000b1' bzcs, '00071280000000b3' u96ict, '0007128000000192' cq, '0007128000000481' rk, '0007128000000482' rksh,
'0007128000000516' cp1, '0007128000000517' cp2, '0007128000000518' cp3, '0007128000000519' wl1, '000712800000051a' wl2,
'0007128000000642' agjyny, '0007128000000643' agjddz, '4800b38000000002' ipqaspec1, '4800b38000000034' ipqaspec2
    ))  
    order by mfgordername asc
    ) t
    left join
(
select c.containername, qd.containerid, sum(qd.qty) qty from qtyhistorydetails qd, container c
where c.containerid = qd.containerid and qd.changeqtytype = '2' group by c.containername, qd.containerid
 ) b
 on t.containerid = b.containerid
    where {0}

group by t.factoryname,t.description, t.salesordername,t.mfgordername,t.moqty,t.dates,t.orderstatusname
", "1 = 1 and t.orderstatusname = '30-正在执行'");

        string sqlString2 = string.Format(@"
select * from (
select t.factoryname,t.description,t.salesordername,t.mfgordername,t.moqty,t.dates,t.orderstatusname,
sum(nvl(t.串焊, '0')) ""串焊"",
sum(nvl(t.层叠, '0')) ""层叠"",
sum(nvl(t.层压, '0')) ""层压"",
sum(nvl(t.装框, '0')) ""装框"",
sum(nvl(t.清洗, '0')) ""清洗"",
sum(nvl(t.IV测试, '0')) + sum(nvl(t.EL测试, '0')) ""IV/EL"",
sum(nvl(t.FI测试, '0')) ""FI"",
sum(nvl(t.包装, '0')) ""包装"",
sum(nvl(t.入库, '0')) ""入库""
from(
select * from(
select
        f.factoryname
       , f.description
        , so.salesordername
       , mo.mfgordername
       , mo.qty moqty
       , c.qty
       , c.containerid
       , upper(sb.specbaseid) specbaseid
       , to_char(mo.plannedstartdate, 'yyyy-mm-dd hh24:Mi:ss') dates
       , os.orderstatusname
       , s.workflowstepname workflowstepname
    from mfgorder mo
   inner
    join jkocir cir on mo.jkocirid = cir.jkocirid

inner
    join salesorder so on so.salesorderid = cir.salesorderid

inner
    join container c on c.mfgorderid = mo.mfgorderid

inner
    join currentstatus cs on cs.currentstatusid = c.currentstatusid

inner
    join spec spec on spec.specid = cs.specid

inner
    join specbase sb on spec.specbaseid = sb.specbaseid

left
    join factory f on f.factoryid = cs.factoryid
inner
    join orderstatus os
  on mo.orderstatusid = os.orderstatusid--工单状态
inner join workflowstep s
    on s.workflowstepid = cs.workflowstepid--工序
   ) temp
    pivot(sum(temp.qty) for workflowstepname in

            (
             '串焊' 串焊,
             '层叠' 层叠,
             '镜检+EL' 镜检EL,
             '层压' 层压,
             '翻转检' 翻转检,
             '装框' 装框,
             '固化' 固化,
             '清洗' 清洗,
             '安规-绝缘耐压' 安规_绝缘耐压,
             '安规-接地电阻' 安规_接地电阻,
             'IV测试' IV测试,
             'EL测试' EL测试,
             '贴铭牌' 贴铭牌,
             'FI测试' FI测试,
             '包装' 包装,
             'OBA' OBA,
             '入库' 入库,
             '入库审核' 入库审核,
             '层前维修' 层前维修,
             '串返修' 串维修,
             '层后返修' 层后返修

            ))
    order by mfgordername asc
    ) t
    left join
(
select c.containername, qd.containerid, sum(qd.qty) qty from qtyhistorydetails qd, container c
where c.containerid = qd.containerid and qd.changeqtytype = '2' group by c.containername, qd.containerid
 ) b
 on t.containerid = b.containerid
    where {0}

group by t.factoryname,t.description, t.salesordername,t.mfgordername,t.moqty,t.dates,t.orderstatusname
) where 1=1 and {1}", "1 = 1 and t.orderstatusname = '30-正在执行'", "串焊<>0 or 包装<>0 or 入库<>0");
        try
        {
            dsResult = dealDate.GetData(sqlString2, out msg);
        }
        catch (Exception ex)
        {
            msg = ex.Message;
        }

        return dsResult;
    }

    //查询成品率(中控室)
    [WebMethod]
    public DataSet GetCenterControl_ProductYield(string strFactory)
    {
        DataSet dsReturn = new DataSet();
        DataTable dtTemp = new DataTable();
        dtTemp.TableName = "ProductYield";
        DateTime dtNow = DateTime.Now;
        dtTemp.Columns.Add("TimeType");
        dtTemp.Columns.Add("TimeValue");
        dtTemp.Columns.Add("TimeStar");
        dtTemp.Columns.Add("TimeEnd");
        dtTemp.Columns.Add("NG_QTY");//不合格数
        dtTemp.Columns.Add("FPY");//一次合格率
        dtTemp.Columns.Add("Product_Yield");//成品率
        dtTemp.AcceptChanges();
        int nQuarter = dtNow.Month / 3;
        nQuarter = nQuarter * 3 < dtNow.Month ? nQuarter + 1 : nQuarter;
        DateTime dtQuarter = DateTime.Parse(DateTime.Now.ToString("yyyy-01-01"));
        for (int i = 1; i <= nQuarter; i++)
        {
            DataRow drQuarter = dtTemp.NewRow();
            DateTime dtQuarterStar = dtQuarter;
            DateTime dtQuarterEnd = dtQuarterStar.AddMonths(3).AddSeconds(-1);
            drQuarter[0] = "Quarter";
            drQuarter[1] = "Q" + i;
            drQuarter[2] = dtQuarterStar.ToString("yyyy-MM-dd HH:mm:ss");
            drQuarter[3] = dtQuarterEnd.ToString("yyyy-MM-dd HH:mm:ss");
            drQuarter = dealDate.GetCenterControlProductTransNew(strFactory, drQuarter, dtQuarterStar, dtQuarterEnd);
            dtTemp.Rows.Add(drQuarter);
            dtQuarter = dtQuarter.AddMonths(3);
        }

        int nStart = -2;
        int nMQuarter = 0;
        for (int i = dtNow.Month - 2; i <= dtNow.Month; i++)
        {
            nMQuarter = i / 3;
            nMQuarter = nMQuarter * 3 < i ? nMQuarter + 1 : nMQuarter;
            if (nMQuarter == nQuarter)
            {
                DataRow drMonth = dtTemp.NewRow();
                drMonth[0] = "Month";
                //drMonth[1] = "M" + i.ToString();
                drMonth[1] = dealDate.GetMonth(i.ToString());
                DateTime dtMonthStart = DateTime.Parse(dtNow.AddMonths(nStart).ToString("yyyy-MM-01"));
                DateTime dtMonthEnd = DateTime.Parse(dtNow.AddMonths(nStart + 1).ToString("yyyy-MM-01")).AddSeconds(-1);
                drMonth[2] = dtMonthStart.ToString("yyyy-MM-dd HH:mm:ss");
                drMonth[3] = dtMonthEnd.ToString("yyyy-MM-dd HH:mm:ss");
                drMonth = dealDate.GetCenterControlProductTransNew(strFactory, drMonth, dtMonthStart, dtMonthEnd);
                dtTemp.Rows.Add(drMonth);
            }
            nStart += 1;
        }
        DataTable dtJkoMonthWeek = dealDate.GetMonthWeek(dtNow);
        for (int i = 0; i < dtJkoMonthWeek.Rows.Count; i++)
        {
            DataRow drWeek = dtTemp.NewRow();
            DateTime dtWeekStart = DateTime.Parse(dtJkoMonthWeek.Rows[i][1].ToString());
            DateTime dtWeekEnd = DateTime.Parse(dtJkoMonthWeek.Rows[i][2].ToString());
            drWeek[0] = "Week";
            drWeek[1] = dealDate.GetMonth(dtNow.Month.ToString()) + "-W" + dtJkoMonthWeek.Rows[i][0];
            drWeek[2] = dtWeekStart.ToString("yyyy-MM-dd HH:mm:ss");
            drWeek[3] = dtWeekEnd.ToString("yyyy-MM-dd HH:mm:ss");
            drWeek = dealDate.GetCenterControlProductTransNew(strFactory, drWeek, dtWeekStart, dtWeekEnd);
            dtTemp.Rows.Add(drWeek);
        }
        DateTime dtDay = dtNow.AddDays(-6);
        for (int i = dtNow.Day - 6; i <= dtNow.Day; i++)
        {
            DataRow drDay = dtTemp.NewRow();
            DateTime dtDayStart = DateTime.Parse(dtDay.ToShortDateString() + " 00:00:00");
            DateTime dtDayEnd = DateTime.Parse(dtDay.ToShortDateString() + " 23:59:59");
            drDay[0] = "Day";
            //drDay[1] = "M" + dtDay.Month+"-D" + dtDay.Day;
            drDay[1] = dealDate.GetMonth(dtDay.Month.ToString()) + "-" + dtDay.Day;
            drDay[2] = dtDayStart.ToString("yyyy-MM-dd HH:mm:ss");
            drDay[3] = dtDayEnd.ToString("yyyy-MM-dd HH:mm:ss");
            drDay = dealDate.GetCenterControlProductTransNew(strFactory, drDay, dtDayStart, dtDayEnd);
            dtTemp.Rows.Add(drDay);
            dtDay = dtDay.AddDays(1);
        }

        dtTemp.AcceptChanges();
        DataTable dtReturn = new DataTable();
        dtReturn.Columns.Add("指标");
        for (int i = 0; i < dtTemp.Rows.Count; i++)
        {
            dtReturn.Columns.Add(dtTemp.Rows[i]["TimeValue"].ToString());
        }
        DataRow drNgQty = dtReturn.NewRow();
        DataRow drFPY = dtReturn.NewRow();
        DataRow drProduct_Yield = dtReturn.NewRow();
        for (int i = 0; i < dtTemp.Rows.Count; i++)
        {
            drNgQty[i + 1] = dtTemp.Rows[i]["NG_QTY"].ToString();
            drFPY[i + 1] = dtTemp.Rows[i]["FPY"].ToString();
            drProduct_Yield[i + 1] = dtTemp.Rows[i]["Product_Yield"].ToString();
        }
        drNgQty[0] = "不合格数";
        drFPY[0] = "一次合格率";
        drProduct_Yield[0] = "成品率";
        dtReturn.Rows.Add(drNgQty);
        dtReturn.Rows.Add(drFPY);
        dtReturn.Rows.Add(drProduct_Yield);
        dtReturn.AcceptChanges();
        dsReturn.Tables.Add(dtReturn);
        return dsReturn;
    }

    //能源消耗（中控室）
    [WebMethod]
    public DataSet GetElectricityList()
    {

        DataSet dsResult = new DataSet();
        string msg = string.Empty;
        string sqlTime = string.Empty;//sql筛选的时间点
        DateTime nowtime = DateTime.Now;
        List<DateTime> timeList = new List<DateTime>();
        timeList.Add(Convert.ToDateTime("0:35"));
        timeList.Add(Convert.ToDateTime("2:05"));
        timeList.Add(Convert.ToDateTime("3:35"));
        timeList.Add(Convert.ToDateTime("5:05"));
        timeList.Add(Convert.ToDateTime("6:35"));
        timeList.Add(Convert.ToDateTime("8:05"));
        timeList.Add(Convert.ToDateTime("9:35"));
        timeList.Add(Convert.ToDateTime("11:05"));
        timeList.Add(Convert.ToDateTime("12:35"));
        timeList.Add(Convert.ToDateTime("14:05"));
        timeList.Add(Convert.ToDateTime("15:35"));
        timeList.Add(Convert.ToDateTime("17:05"));
        timeList.Add(Convert.ToDateTime("18:35"));
        timeList.Add(Convert.ToDateTime("20:05"));
        timeList.Add(Convert.ToDateTime("21:35"));
        timeList.Add(Convert.ToDateTime("23:05"));

        for (int i = 0; i < timeList.Count; i++)
        {
            if (nowtime >= timeList[i])
            {
                sqlTime = timeList[i].AddMinutes(-1).ToString("yyyy/MM/dd HH:mm");
            }
            else if (i == 0)
            {
                sqlTime = timeList.Last().AddDays(-1).AddMinutes(-1).ToString("yyyy/MM/dd HH:mm");
                break;
            }
            else
            {
                sqlTime = timeList[i - 1].AddMinutes(-1).ToString("yyyy/MM/dd HH:mm");
                break;
            }
        }

        string sqlString = string.Format(@"  select 
 fa.factoryname,
 fa.description,
 rt.resourcetypename,
sum(nvl(dphd.datavalue,'0')) total 

from datapointhistory dph

left join datacollectiondef dcd on dcd.datacollectiondefid=dph.datacollectiondefid
inner join datacollectiondefbase dcdb on dcd.datacollectiondefbaseid=dcdb.datacollectiondefbaseid
    and dcd.datacollectiondefid=replace(dcd.datacollectiondefid,'0000000000000000',dcdb.revofrcdid)

inner join datapointhistorydetail dphd on dph.datapointhistoryid=dphd.datapointhistoryid   
left join historymainline hml on hml.historymainlineid=dph.historymainlineid
left join resourcedef rd on rd.resourceid=hml.resourceid
left join factory fa on fa.factoryid=rd.factoryid
left join resourcefamily rf on rf.resourcefamilyid=rd.resourcefamilyid
left join resourcetype rt on rt.resourcetypeid=rd.resourcetypeid
    where  dcdb.datacollectiondefname ='{0}'
    group by fa.factoryname,fa.description, rt.resourcetypename
", "能耗数据采集");
        string sqlString2 = string.Format(@"select hm.factoryname,hm.description1,hm.resourcetypename,sum(nvl(hm.datavalue,'0')) total from (
    select 
    fa.factoryname,
    fa.description description1,
    rt.resourcetypename,
    --sum(nvl(dphd.datavalue,'0')) total, 
    dphd.datavalue,
    hml.txndate,
    rd.resourcename,
    rd.description
    from datapointhistory dph
    left join datacollectiondef dcd on dcd.datacollectiondefid=dph.datacollectiondefid
    inner join datacollectiondefbase dcdb on dcd.datacollectiondefbaseid=dcdb.datacollectiondefbaseid
        and dcd.datacollectiondefid=replace(dcd.datacollectiondefid,'0000000000000000',dcdb.revofrcdid)
    inner join datapointhistorydetail dphd on dph.datapointhistoryid=dphd.datapointhistoryid   
    left join historymainline hml on hml.historymainlineid=dph.historymainlineid
    left join resourcedef rd on rd.resourceid=hml.resourceid
    left join factory fa on fa.factoryid=rd.factoryid
    left join resourcefamily rf on rf.resourcefamilyid=rd.resourcefamilyid
    left join resourcetype rt on rt.resourcetypeid=rd.resourcetypeid
    where  dcdb.datacollectiondefname ='{0}' and rt.resourcetypename is not null and dphd.datavalue!=0 and to_char(hml.txndate,'yyyy/mm/dd hh24:mi')='{1}'
    order by hml.txndate desc
    ) hm group by hm.factoryname,hm.description1,hm.resourcetypename
", "能耗数据采集", sqlTime);
        try
        {
            DataSet ds = dealDate.GetData(sqlString2, out msg);
            dsResult = ds.Clone();
            dsResult.Clear();

            if (ds != null && ds.Tables[0].Rows.Count > 0)
            {

                DataRow[] drs = ds.Tables[0].Select();
                for (int i = 0; i < drs.Length; i++)
                {
                    if (drs[i]["resourcetypename"].ToString().Equals("IT  UPS用电") || drs[i]["resourcetypename"].ToString().Equals("照明设备"))
                    {
                        continue;
                    }
                    if (drs[i]["resourcetypename"].ToString().Equals("生产设备"))
                    {
                        string strFactory = drs[i]["factoryname"].ToString();
                        DataRow[] drr = ds.Tables[0].Select("factoryname = '" + strFactory + "' and resourcetypename = '照明设备'");
                        if (drr.Length > 0)
                        {
                            drs[i]["total"] = (Convert.ToDecimal(drs[i]["total"]) + Convert.ToDecimal(drr[0]["total"]));
                        }
                        else
                        {
                            drs[i]["total"] = Convert.ToDecimal(drs[i]["total"]);
                        }
                    }
                    //dsResult.Tables[0].ImportRow(drs[i]);
                    var oneRow = drs[i];
                    oneRow[3] = Convert.ToDouble(oneRow[3]) * 10;
                    dsResult.Tables[0].ImportRow(oneRow);
                }
            }

        }
        catch (Exception ex)
        {
            msg = ex.Message;
        }

        return dsResult;
    }

}
