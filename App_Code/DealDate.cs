using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Configuration;

/// <summary>
/// DealDate 的摘要说明
/// </summary>
public class DealDate
{
    public DealDate()
    {
        //
        // TODO: 在此处添加构造函数逻辑
        //
    }
    //获取月份
    public string GetMonth(string spMonth)
    {
        string sMonth = Convert.ToInt32(spMonth).ToString();
        string sMonthEn = string.Empty;
        switch (sMonth)
        {
            case "1":
                sMonthEn = "Jan";
                break;
            case "2":
                sMonthEn = "Feb";
                break;
            case "3":
                sMonthEn = "Mar";
                break;
            case "4":
                sMonthEn = "Apr";
                break;
            case "5":
                sMonthEn = "May";
                break;
            case "6":
                sMonthEn = "Jun";
                break;
            case "7":
                sMonthEn = "Jul";
                break;
            case "8":
                sMonthEn = "Aug";
                break;
            case "9":
                sMonthEn = "Sep";
                break;
            case "10":
                sMonthEn = "Oct";
                break;
            case "11":
                sMonthEn = "Nov";
                break;
            case "12":
                sMonthEn = "Dec";
                break;
            default:
                break;
        }
        return sMonthEn;
    }
    //处理获取到的数据
    public DataSet GetData(string OracleSQL, out string msg)
    {
        DataSet resDS = new DataSet();
        msg = string.Empty;
        using (OracleConnection conn = new OracleConnection())
        {
            conn.ConnectionString = ConfigurationManager.ConnectionStrings["MES"].ConnectionString;
            conn.Open();
            OracleCommand comm = conn.CreateCommand();
            comm.CommandTimeout = 1200000;
            comm.CommandText = OracleSQL;
            comm.CommandType = CommandType.Text;
            OracleDataAdapter oda = new OracleDataAdapter(comm);
            oda.Fill(resDS);
            conn.Close();
            conn.Dispose();
            //try
            //{
            //    conn.ConnectionString = ConfigurationManager.ConnectionStrings["MES"].ConnectionString;
            //    conn.Open();
            //    OracleCommand comm = conn.CreateCommand();
            //    comm.CommandText = OracleSQL;
            //    comm.CommandType = CommandType.Text;
            //    OracleDataAdapter oda = new OracleDataAdapter(comm);
            //    oda.Fill(resDS);
            //}
            //catch (Exception ex)
            //{
            //    msg = ex.Message;
            //    throw ex;
            //}
            //finally
            //{
            //    conn.Close();
            //    conn.Dispose();
            //}
        }
        return resDS;
    }

    #region by车间代码查询成品率、一次合格率、不合格数
    /// <summary>
    /// 截止查询时间点当月所有周别，及每周起止时间
    /// </summary>
    /// <param name="dtTime"></param>
    /// <returns></returns>
    public DataTable GetMonthWeek(DateTime dtTime)
    {
        DataSet dsResult = new DataSet();
        string msg = string.Empty;
        string sqlString = string.Empty;
        //sqlString += "select  to_char(to_date('" + dtTime.ToString("yyyyMMdd") + " 23:59:59','yyyyMMdd HH24:mi:ss'),'MM')||'-W'||monthweek  ,min(dtstar) as dtstar,max(dtend) as dtend from (";
        sqlString += "select  monthweek  ,min(dtstar) as dtstar,max(dtend) as dtend from (";
        DateTime dtMonthStar = dtTime.AddDays(1 - dtTime.Day);
        for (int i = 1; i <= dtTime.Day; i++)
        {
            sqlString += "   select ";
            sqlString += " to_char(to_date('" + dtMonthStar.ToString("yyyyMMdd") + " 23:59:59', 'yyyyMMdd HH24:mi:ss'), 'iw')- ";
            sqlString += "  to_char(last_day(add_months(to_date('" + dtMonthStar.ToString("yyyyMMdd") + " 23:59:59', 'yyyyMMdd HH24:mi:ss'), -1)) + 1, 'iw') + 1 as monthweek, ";
            sqlString += " to_date('" + dtMonthStar.ToString("yyyyMMdd") + " 00:00:00', 'yyyyMMdd HH24:mi:ss') as dtStar, ";
            sqlString += " to_date('" + dtMonthStar.ToString("yyyyMMdd") + " 23:59:59', 'yyyyMMdd HH24:mi:ss') as dtend ";
            sqlString += " from dual ";
            sqlString += " union";
            dtMonthStar = dtMonthStar.AddDays(1);

        }
        sqlString = sqlString.Substring(0, sqlString.Length - 5);
        sqlString += ") A group by monthweek  order by monthweek";
        try
        {
            dsResult = GetData(sqlString, out msg);
        }
        catch (Exception ex)
        {
            msg = ex.Message;
        }

        return dsResult.Tables[0];
    }

    public DataRow GetCenterControlProductTrans(string strFactory, DataRow drTemp, DateTime dtYearStar, DateTime dtYearEnd)
    {

        //一次合格率=一次合格数/检验总数
        float fFpyQty = 0;
        string strFpyQty = GetCenterControlYieldData_FPYQty(dtYearStar, dtYearEnd, strFactory).Rows[0][0].ToString();
        float.TryParse(strFpyQty, out fFpyQty);
        //检验总数
        float fTotalQty = 0;
        string strTotalQty = GetCenterControlYieldData_TotalQty(dtYearStar, dtYearEnd, strFactory).Rows[0][0].ToString();
        float.TryParse(strTotalQty, out fTotalQty);
        float fpyYield = fFpyQty * 1.0f / fTotalQty * 100;
        drTemp[5] = fpyYield.ToString("N2") + "%";//一次合格率
                                                  //返工降级数
        float fDownGrade = 0;
        string strFownGradeQty = GetCenterControlYieldData_DownGrade(dtYearStar, dtYearEnd, strFactory).Rows[0][0].ToString();
        float.TryParse(strFownGradeQty, out fDownGrade);
        //不合格数
        float fNg = 0;
        string strNg = GetCenterControlYieldData_NG(dtYearStar, dtYearEnd, strFactory).Rows[0][0].ToString();
        float.TryParse(strNg, out fNg);
        drTemp[4] = strNg;
        //报废数
        float fLoss = 0;
        string strLoss = GetCenterControlYieldData_Loss(dtYearStar, dtYearEnd, strFactory).Rows[0][0].ToString();
        float.TryParse(strLoss, out fLoss);
        //确认合格数
        float fConfirmOk = 0;
        string strConfirmOk = GetCenterControlYieldData_ConfirmOK(dtYearStar, dtYearEnd, strFactory).Rows[0][0].ToString();
        float.TryParse(strConfirmOk, out fConfirmOk);
        //成品率=1-（（返工降级数+不合格数+报废数-确认合格数）/检验总数）
        fDownGrade = fDownGrade + fNg + fLoss - fConfirmOk;
        fDownGrade = 1 - fDownGrade * 1.0f / fTotalQty;
        fDownGrade *= 100;
        drTemp[6] = fDownGrade.ToString("N2") + "%";
        return drTemp;

    }

    public DataRow GetCenterControlProductTransNew(string strFactory, DataRow drTemp, DateTime dtYearStar, DateTime dtYearEnd)
    {
        DataTable queryData = GetCenterControlYieldData_AllData(dtYearStar, dtYearEnd);
        float fFpyQty = 0;
        float fTotalQty = 0;
        float fNg = 0;
        float fLoss = 0;
        float fConfirmOk = 0;
        float fpyYield = 0;
        float fDownGrade = 0;
        if (strFactory == "MS0021")
        {
            //0,2,4,6,8,10
            float.TryParse(queryData.Rows[0][0].ToString(), out fFpyQty);
            float.TryParse(queryData.Rows[0][2].ToString(), out fTotalQty);
            float.TryParse(queryData.Rows[0][4].ToString(), out fDownGrade);
            float.TryParse(queryData.Rows[0][6].ToString(), out fNg);
            float.TryParse(queryData.Rows[0][8].ToString(), out fLoss);
            float.TryParse(queryData.Rows[0][10].ToString(), out fConfirmOk);

            //不合格数
            drTemp[4] = queryData.Rows[0][6].ToString();

            //一次合格率=一次合格数/检验总数
            fpyYield = fFpyQty * 1.0f / fTotalQty * 100;
            if (float.IsNaN(fpyYield))
            {
                drTemp[5] = "100%";
            }
            else
            {
                drTemp[5] = fpyYield.ToString("N2") + "%";
            }
            
            //成品率=1-（（返工降级数+不合格数+报废数-确认合格数）/检验总数）
            fDownGrade = fDownGrade + fNg + fLoss - fConfirmOk;
            fDownGrade = 1 - fDownGrade * 1.0f / fTotalQty;
            fDownGrade *= 100;
            if (float.IsNaN(fDownGrade))
            {
                drTemp[6] = "100%";
            }
            else
            {
                drTemp[6] = fDownGrade.ToString("N2") + "%";
            }
        }
        else if (strFactory == "MS0022")
        {
            //1,3,5,7,9,11
            float.TryParse(queryData.Rows[0][1].ToString(), out fFpyQty);
            float.TryParse(queryData.Rows[0][3].ToString(), out fTotalQty);
            float.TryParse(queryData.Rows[0][5].ToString(), out fDownGrade);
            float.TryParse(queryData.Rows[0][7].ToString(), out fNg);
            float.TryParse(queryData.Rows[0][9].ToString(), out fLoss);
            float.TryParse(queryData.Rows[0][11].ToString(), out fConfirmOk);

            //不合格数
            drTemp[4] = queryData.Rows[0][7].ToString();

            //一次合格率=一次合格数/检验总数
            fpyYield = fFpyQty * 1.0f / fTotalQty * 100;
            if (float.IsNaN(fpyYield))
            {
                drTemp[5] = "100%";
            }
            else
            {
                drTemp[5] = fpyYield.ToString("N2") + "%";
            }

            //成品率=1-（（返工降级数+不合格数+报废数-确认合格数）/检验总数）
            fDownGrade = fDownGrade + fNg + fLoss - fConfirmOk;
            fDownGrade = 1 - fDownGrade * 1.0f / fTotalQty;
            fDownGrade *= 100;
            if (float.IsNaN(fDownGrade))
            {
                drTemp[6] = "100%";
            }
            else
            {
                drTemp[6] = fDownGrade.ToString("N2") + "%";
            }
        }
        return drTemp;

    }
    /// <summary>
    /// 中控大屏成品率查询-返工组降级数
    /// </summary>
    /// <param name="dtStart"></param>
    /// <param name="dtEnd"></param>
    /// <param name="strFactory"></param>
    /// <returns></returns>
    public DataTable GetCenterControlYieldData_DownGrade(DateTime dtStart, DateTime dtEnd, string strFactory)
    {
        DataSet dsResult = new DataSet();
        string msg = string.Empty;
        string sqlString = string.Empty;
        sqlString += " with NormalFIOK as";
        sqlString += " (select * ";
        sqlString += "  from(SELECT row_number() over(PARTITION BY fihis.historyid ORDER BY hm.txndate DESC) AS rowflag, ";
        sqlString += "  fihis.jkofichecktr, ";
        sqlString += "  fihis.historyid as ContainerID, ";
        sqlString += "   hm.txndate ";
        sqlString += "  from jkofitesthistory fihis, historymainline hm, factory f ";
        sqlString += " where fihis.historymainlineid = hm.historymainlineid ";
        sqlString += "  and hm.factoryid = f.factoryid ";
        sqlString += "  and f.factoryname = '" + strFactory + "' ";
        sqlString += "  and hm.txndate >= ";
        sqlString += "      to_date('" + dtStart.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')";
        sqlString += "  and hm.txndate <= ";
        sqlString += "      to_date('" + dtEnd.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')) A ";
        sqlString += "  where A.rowflag = 1 ";
        sqlString += "  and A.jkofichecktr in ('OK', 'JK')), ";
        sqlString += " FANGONG AS ";
        sqlString += " (SELECT C.Containerid, c.containername, RGH.JKOFACTORYBEFORE WorkCentername ";
        sqlString += " FROM JKOREWORKGROUPHISTORY     RGH, ";
        sqlString += "    ASSOCIATEHISTORY          AH, ";
        sqlString += "   ASSOCIATEHISTORYCHILDCNTS ACH, ";
        sqlString += "  CONTAINER                 C ";
        sqlString += " WHERE RGH.JKOISRETURN = 0 ";
        sqlString += "  AND RGH.JKOPALLETID = AH.PARENTCONTAINERID ";
        sqlString += "  AND AH.ASSOCIATEHISTORYID = ACH.ASSOCIATEHISTORYID ";
        sqlString += "  AND ACH.CHILDCONTAINERSID = C.CONTAINERID), ";
        sqlString += " FGFING as ";
        sqlString += "  (select * ";
        sqlString += "  from(SELECT row_number() over(PARTITION BY fihis.historyid ORDER BY hm.txndate DESC) AS rowflag, ";
        sqlString += "        fihis.jkofichecktr, ";
        sqlString += "  fihis.historyid as ContainerID, ";
        sqlString += "  hm.txndate ";
        sqlString += "  from jkofitesthistory fihis, historymainline hm, factory f ";
        sqlString += "  where fihis.historymainlineid = hm.historymainlineid ";
        sqlString += "  and hm.factoryid = f.factoryid ";
        sqlString += "  and f.factoryname = 'MSF001' ";
        sqlString += "  and hm.txndate >= ";
        sqlString += "    to_date('" + dtStart.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss') ";
        sqlString += "   and hm.txndate <= ";
        sqlString += "     to_date('" + dtEnd.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')) A ";
        sqlString += "  where A.rowflag = 1 ";
        sqlString += "   and A.jkofichecktr = 'NG') ";
        sqlString += " select count(FANGONG.Containerid) as FANGONGZU_Qty ";
        sqlString += "  from NormalFIOK, FANGONG, FGFING ";
        sqlString += "  where NormalFIOK.Containerid = FANGONG.Containerid ";
        sqlString += "   and FGFING.Containerid = FANGONG.Containerid ";
        sqlString += "   and FGFING.txndate > NormalFIOK.txndate ";

        string sqlString1 = string.Format(@"
                with NormalFIOK as 
                (select * from(
                SELECT row_number() over(PARTITION BY fihis.historyid ORDER BY hm.txndate DESC) AS rowflag,   fihis.jkofichecktr,   fihis.historyid as ContainerID,    hm.txndate 
                from jkofitesthistory fihis
                inner join historymainline hm on fihis.historymainlineid=hm.historymainlineid
                inner join factory f on f.factoryid=hm.factoryid
                left join container c on c.containerid=hm.containerid 
                left join mfgorder mo on c.mfgorderid =mo.mfgorderid
                left join ordertype ot on ot.ordertypeid = mo.ordertypeid --工单类型
                where fihis.historymainlineid = hm.historymainlineid
                and hm.factoryid = f.factoryid
                and f.factoryname = '{0}'
                and hm.txndate >= to_date('{1}', 'yyyy-mm-dd hh24:mi:ss')
                and hm.txndate <= to_date('{2}', 'yyyy-mm-dd hh24:mi:ss')
                and ot.ordertypename!='ZR16') A
                where A.rowflag = 1 and A.jkofichecktr in ('OK', 'JK')),
                FANGONG AS 
                (SELECT C.Containerid, c.containername, RGH.JKOFACTORYBEFORE WorkCentername
                FROM JKOREWORKGROUPHISTORY RGH,ASSOCIATEHISTORY AH,ASSOCIATEHISTORYCHILDCNTS ACH,CONTAINER C
                WHERE RGH.JKOISRETURN = 0
                AND RGH.JKOPALLETID = AH.PARENTCONTAINERID
                AND AH.ASSOCIATEHISTORYID = ACH.ASSOCIATEHISTORYID
                AND ACH.CHILDCONTAINERSID = C.CONTAINERID),
                FGFING as 
                (select * from(
                SELECT row_number() over(PARTITION BY fihis.historyid ORDER BY hm.txndate DESC) AS rowflag,
                fihis.jkofichecktr,
                fihis.historyid as ContainerID,
                hm.txndate
                from jkofitesthistory fihis, historymainline hm, factory f
                where fihis.historymainlineid = hm.historymainlineid
                and hm.factoryid = f.factoryid
                and f.factoryname = '{0}'
                and hm.txndate >= to_date('{1}', 'yyyy-mm-dd hh24:mi:ss')
                and hm.txndate <= to_date('{2}', 'yyyy-mm-dd hh24:mi:ss')) A
                where A.rowflag = 1    and A.jkofichecktr = 'NG')

                select count(FANGONG.Containerid) as FANGONGZU_Qty
                from NormalFIOK, FANGONG, FGFING
                where NormalFIOK.Containerid = FANGONG.Containerid
                and FGFING.Containerid = FANGONG.Containerid
                and FGFING.txndate > NormalFIOK.txndate ", strFactory, dtStart.ToString("yyyy-MM-dd HH:mm:ss"), dtEnd.ToString("yyyy-MM-dd HH:mm:ss"));
        try
        {
            dsResult = GetData(sqlString1, out msg);
        }
        catch (Exception ex)
        {
            msg = ex.Message;
        }
        return dsResult.Tables[0];

    }
    /// <summary>
    /// 中控大屏成品率查询-不合格数
    /// </summary>
    /// <param name="dtStart"></param>
    /// <param name="dtEnd"></param>
    /// <param name="strFactory"></param>
    /// <returns></returns>
    public DataTable GetCenterControlYieldData_NG(DateTime dtStart, DateTime dtEnd, string strFactory)
    {
        DataSet dsResult = new DataSet();
        string msg = string.Empty;
        string sqlString = string.Empty;
        sqlString += "  SELECT count(distinct d.containerid) as NGQty";
        sqlString += "  FROM(SELECT row_number() over(PARTITION BY fihis.historyid ORDER BY hm.txndate DESC) AS rowflag,";
        sqlString += "  fihis.jkofichecktr,";
        sqlString += "  fihis.historyid,";
        sqlString += "  hm.txndate,";
        sqlString += "  hm.txnid";
        sqlString += "  FROM historymainline hm, jkofitesthistory fihis, factory f";
        sqlString += " WHERE hm.historymainlineid = fihis.historymainlineid and hm.factoryid=f.factoryid ";
        sqlString += "  and f.factoryname = '" + strFactory + "' ";
        sqlString += "  and hm.txndate >= ";
        sqlString += "      to_date('" + dtStart.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')";
        sqlString += "  and hm.txndate <= ";
        sqlString += "      to_date('" + dtEnd.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')";
        sqlString += "  ) fidata,";
        sqlString += "  containerdefecthistorydetail d,";
        sqlString += "  containerdefectreason r";
        sqlString += "  WHERE fidata.rowflag = 1";
        sqlString += "  AND fidata.jkofichecktr = 'NG'";
        sqlString += "  AND fidata.historyid = d.containerid";
        sqlString += "  AND fidata.txnid = d.txnid";
        sqlString += "  AND d.reasoncodeid = r.defectreasonid";
        sqlString += "  AND substr(r.defectreasonname, 1, 2) <> 'FG'";

        string sqlString1 = string.Format(@"
                SELECT count(d.containerid) as NGQty
                FROM(
                SELECT row_number() over(PARTITION BY fihis.historyid ORDER BY hm.txndate DESC) AS rowflag,
                fihis.jkofichecktr,
                fihis.historyid,
                hm.txndate,
                hm.txnid
                from jkofitesthistory fihis
                inner join historymainline hm on fihis.historymainlineid=hm.historymainlineid
                inner join factory f on f.factoryid=hm.factoryid
                left join container c on c.containerid=hm.containerid 
                left join mfgorder mo on c.mfgorderid =mo.mfgorderid
                left join ordertype ot on ot.ordertypeid = mo.ordertypeid --工单类型
                WHERE hm.historymainlineid = fihis.historymainlineid
                and hm.factoryid=f.factoryid
                and f.factoryname = '{0}'
                and hm.txndate >= to_date('{1}', 'yyyy-mm-dd hh24:mi:ss')
                and hm.txndate <= to_date('{2}', 'yyyy-mm-dd hh24:mi:ss')
                and ot.ordertypename!='ZR16') fidata,
                containerdefecthistorydetail d,
                containerdefectreason r
                WHERE fidata.rowflag = 1
                AND fidata.jkofichecktr = 'NG'
                AND fidata.historyid = d.containerid
                AND fidata.txnid = d.txnid
                AND d.reasoncodeid = r.defectreasonid
                AND substr(r.defectreasonname, 1, 2) <> 'FG'", strFactory, dtStart.ToString("yyyy-MM-dd HH:mm:ss"), dtEnd.ToString("yyyy-MM-dd HH:mm:ss"));
        try
        {
            dsResult = GetData(sqlString1, out msg);
        }
        catch (Exception ex)
        {
            msg = ex.Message;
        }
        return dsResult.Tables[0];
    }
    /// <summary>
    /// 中控大屏成品率查询-报废数
    /// </summary>
    /// <param name="dtStart"></param>
    /// <param name="dtEnd"></param>
    /// <param name="strFactory"></param>
    /// <returns></returns>
    public DataTable GetCenterControlYieldData_Loss(DateTime dtStart, DateTime dtEnd, string strFactory)
    {
        DataSet dsResult = new DataSet();
        string msg = string.Empty;
        string sqlString = string.Empty;

        sqlString += "  select count(distinct qhd.containerid) as LossQty";
        sqlString += "  from qtyhistorydetails qhd, historymainline hm, factory f";
        sqlString += "  where qhd.changeqtytype = '2'";
        sqlString += "  and hm.txnid = qhd.txnid";
        sqlString += "  and hm.factoryid = f.factoryid";
        sqlString += "  and f.factoryname = '" + strFactory + "' ";
        sqlString += "  and hm.txndate >= ";
        sqlString += "      to_date('" + dtStart.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')";
        sqlString += "  and hm.txndate <= ";
        sqlString += "      to_date('" + dtEnd.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')";

        string sqlString1 = string.Format(@"
                select count(qhd.containerid) as LossQty
                from qtyhistorydetails qhd
                inner join historymainline hm on qhd.historyid=hm.historyid
                inner join factory f on f.factoryid=hm.factoryid
                left join container c on c.containerid=hm.containerid 
                left join mfgorder mo on c.mfgorderid =mo.mfgorderid
                left join ordertype ot on ot.ordertypeid = mo.ordertypeid --工单类型
                where qhd.changeqtytype = '2'
                and hm.txnid = qhd.txnid
                and hm.factoryid = f.factoryid
                and f.factoryname = '{0}'
                and hm.txndate >= to_date('{1}', 'yyyy-mm-dd hh24:mi:ss')
                and hm.txndate <= to_date('{2}', 'yyyy-mm-dd hh24:mi:ss')
                and ot.ordertypename!='ZR16'", strFactory, dtStart.ToString("yyyy-MM-dd HH:mm:ss"), dtEnd.ToString("yyyy-MM-dd HH:mm:ss"));
        try
        {
            dsResult = GetData(sqlString1, out msg);
        }
        catch (Exception ex)
        {
            msg = ex.Message;
        }
        return dsResult.Tables[0];
    }
    /// <summary>
    /// 中控大屏成品率查询-确认合格数
    /// </summary>
    /// <param name="dtStart"></param>
    /// <param name="dtEnd"></param>
    /// <param name="strFactory"></param>
    /// <returns></returns>
    public DataTable GetCenterControlYieldData_ConfirmOK(DateTime dtStart, DateTime dtEnd, string strFactory)
    {
        DataSet dsResult = new DataSet();
        string msg = string.Empty;
        string sqlString = string.Empty;

        sqlString += "  with FING as";
        sqlString += "  (select *";
        sqlString += "   from(SELECT row_number() over(PARTITION BY fihis.historyid ORDER BY hm.txndate DESC) AS rowflag,";
        sqlString += " fihis.jkofichecktr,";
        sqlString += " fihis.historyid as ContainerID,";
        sqlString += " hm.txndate";
        sqlString += " from jkofitesthistory fihis, historymainline hm, factory f";
        sqlString += "  where fihis.historymainlineid = hm.historymainlineid";
        sqlString += " and hm.factoryid = f.factoryid";
        sqlString += "  and f.factoryname = '" + strFactory + "' ";
        sqlString += "  and hm.txndate >= ";
        sqlString += "      to_date('" + dtStart.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')";
        sqlString += "  and hm.txndate <= ";
        sqlString += "      to_date('" + dtEnd.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')) A ";
        sqlString += " where A.rowflag = 1";
        sqlString += " and A.jkofichecktr = 'NG'),";
        sqlString += " DGOK as";
        sqlString += " (select *";
        sqlString += "    from(SELECT row_number() over(PARTITION BY dghis.jkocontainerid ORDER BY dghis.txndate DESC) AS rowflag,";
        sqlString += "  dghis.jkomrbresult,";
        sqlString += "  dghis.jkocontainerid as ContainerID,";
        sqlString += "   dghis.txndate";
        sqlString += "  from jkoDownGradeHistory dghis";
        sqlString += " where dghis.txndate >=";
        sqlString += "     to_date('" + dtStart.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')";
        sqlString += "  and dghis.txndate <=";
        sqlString += "    to_date('" + dtEnd.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')) B";
        sqlString += "   where B.rowflag = 1";
        sqlString += "   and B.jkomrbresult in ('JK', 'OK'))";
        sqlString += " select count(distinct FING.ContainerID) AS Confirm_OKQty";
        sqlString += "  from FING, DGOK";
        sqlString += " where FING.ContainerID = DGOK.ContainerID";
        sqlString += "  and DGOK.txndate >= FING.txndate";


        string sqlString1 = string.Format(@"
                with FING as
                (select * from
                (SELECT row_number() over(PARTITION BY fihis.historyid ORDER BY hm.txndate DESC) AS rowflag,
                fihis.jkofichecktr,
                fihis.historyid as ContainerID,
                hm.txndate 
                from jkofitesthistory fihis
                inner join historymainline hm on fihis.historymainlineid=hm.historymainlineid
                inner join factory f on f.factoryid=hm.factoryid
                left join container c on c.containerid=hm.containerid 
                left join mfgorder mo on c.mfgorderid =mo.mfgorderid
                left join ordertype ot on ot.ordertypeid = mo.ordertypeid --工单类型
                where fihis.historymainlineid = hm.historymainlineid
                and hm.factoryid = f.factoryid
                and f.factoryname = '{0}'
                and hm.txndate >= to_date('{1}', 'yyyy-mm-dd hh24:mi:ss')
                and hm.txndate <= to_date('{2}', 'yyyy-mm-dd hh24:mi:ss')
                and ot.ordertypename!='ZR16') A
                where A.rowflag = 1 and A.jkofichecktr = 'NG'),
                DGOK as (
                select * from
                (SELECT row_number() over(PARTITION BY dghis.jkocontainerid ORDER BY dghis.txndate DESC) AS rowflag,
                dghis.jkomrbresult,
                dghis.jkocontainerid as ContainerID,
                dghis.txndate
                from jkoDownGradeHistory dghis
                where dghis.txndate >= to_date('{3}', 'yyyy-mm-dd hh24:mi:ss')
                and dghis.txndate <= to_date('{4}', 'yyyy-mm-dd hh24:mi:ss')) B
                where B.rowflag = 1
                and B.jkomrbresult in ('JK', 'OK'))

                select count(distinct FING.ContainerID) AS Confirm_OKQty
                from FING, DGOK
                where FING.ContainerID = DGOK.ContainerID
                and DGOK.txndate >= FING.txndate", strFactory, dtStart.ToString("yyyy-MM-dd HH:mm:ss"), dtEnd.ToString("yyyy-MM-dd HH:mm:ss"), dtStart.ToString("yyyy-MM-dd HH:mm:ss"), dtEnd.ToString("yyyy-MM-dd HH:mm:ss"));
        try
        {
            dsResult = GetData(sqlString1, out msg);
        }
        catch (Exception ex)
        {
            msg = ex.Message;
        }
        return dsResult.Tables[0];
    }
    /// <summary>
    /// 中控大屏成品率查询-检验总数
    /// </summary>
    /// <param name="dtStart"></param>
    /// <param name="dtEnd"></param>
    /// <param name="strFactory"></param>
    /// <returns></returns>
    public DataTable GetCenterControlYieldData_TotalQty(DateTime dtStart, DateTime dtEnd, string strFactory)
    {
        DataSet dsResult = new DataSet();
        string msg = string.Empty;
        string sqlString = string.Empty;

        sqlString += " select count(distinct fihis.historyid) AS TestQty";
        sqlString += "   from jkofitesthistory fihis, historymainline hm, factory f";
        sqlString += " where fihis.historymainlineid = hm.historymainlineid";
        sqlString += "   and hm.factoryid = f.factoryid";
        sqlString += "  and f.factoryname = '" + strFactory + "' ";
        sqlString += "  and hm.txndate >= ";
        sqlString += "      to_date('" + dtStart.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')";
        sqlString += "  and hm.txndate <= ";
        sqlString += "      to_date('" + dtEnd.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')";

        string sqlString1 = string.Format(@"
                select count(fihis.historyid) AS TestQty
                from jkofitesthistory fihis
                    inner join historymainline hm on fihis.historymainlineid=hm.historymainlineid
                    inner join factory f on f.factoryid=hm.factoryid
                    inner join container c on c.containerid=hm.containerid 
                    inner join mfgorder mo on c.mfgorderid =mo.mfgorderid
                    inner join ordertype ot on ot.ordertypeid = mo.ordertypeid --工单类型
                where fihis.historymainlineid = hm.historymainlineid
                    and hm.factoryid = f.factoryid  and f.factoryname = '{0}'
                    and hm.txndate >= to_date('{1}', 'yyyy-mm-dd hh24:mi:ss')
                    and hm.txndate <= to_date('{2}', 'yyyy-mm-dd hh24:mi:ss')
                    and ot.ordertypename!='ZR16'", strFactory, dtStart.ToString("yyyy-MM-dd HH:mm:ss"), dtEnd.ToString("yyyy-MM-dd HH:mm:ss"));
        try
        {
            dsResult = GetData(sqlString1, out msg);
        }
        catch (Exception ex)
        {
            msg = ex.Message;
        }
        return dsResult.Tables[0];
    }
    /// <summary>
    /// 中控大屏成品率查询-一次合格数
    /// </summary>
    /// <param name="dtStart"></param>
    /// <param name="dtEnd"></param>
    /// <param name="strFactory"></param>
    /// <returns></returns>
    public DataTable GetCenterControlYieldData_FPYQty(DateTime dtStart, DateTime dtEnd, string strFactory)
    {
        DataSet dsResult = new DataSet();
        string msg = string.Empty;
        string sqlString = string.Empty;

        sqlString += "select count (*) AS FPYQty";
        sqlString += " from(SELECT row_number() over(PARTITION BY fihis.historyid ORDER BY hm.txndate ASC) AS rowflag, ";
        sqlString += " fihis.jkofichecktr, ";
        sqlString += " fihis.historyid as ContainerID, ";
        sqlString += " hm.txndate";
        sqlString += " from jkofitesthistory fihis, historymainline hm, factory f";
        sqlString += " where fihis.historymainlineid = hm.historymainlineid";
        sqlString += " and hm.factoryid = f.factoryid ";
        sqlString += "  and f.factoryname = '" + strFactory + "' ";
        sqlString += " and hm.txndate >= ";
        sqlString += "   to_date('" + dtStart.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')";
        sqlString += " and hm.txndate <= ";
        sqlString += "  to_date('" + dtEnd.ToString("yyyy-MM-dd HH:mm:ss") + "', 'yyyy-mm-dd hh24:mi:ss')) A ";
        sqlString += " where A.rowflag = 1 ";
        sqlString += " and A.jkofichecktr in('JK', 'OK') ";

        string sqlString1 = string.Format(@"
                select count (*) AS FPYQty from(
                SELECT  row_number() over(PARTITION BY fihis.historyid ORDER BY hm.txndate ASC) AS rowflag,
                        fihis.jkofichecktr,
                        fihis.historyid as ContainerID,
                        hm.txndate 
                from jkofitesthistory fihis
                inner join historymainline hm on fihis.historymainlineid=hm.historymainlineid
                inner join factory f on f.factoryid=hm.factoryid
                inner join container c on c.containerid=hm.containerid 
                inner join mfgorder mo on c.mfgorderid =mo.mfgorderid
                inner join ordertype ot on ot.ordertypeid = mo.ordertypeid --工单类型
                where fihis.historymainlineid = hm.historymainlineid
                and hm.factoryid = f.factoryid
                and f.factoryname = '{0}'
                and hm.txndate >= to_date('{1}', 'yyyy-mm-dd hh24:mi:ss')
                and hm.txndate <=   to_date('{2}', 'yyyy-mm-dd hh24:mi:ss')
                and ot.ordertypename!='ZR16'
                 )A  where A.rowflag = 1  and A.jkofichecktr in('JK', 'OK')", strFactory, dtStart.ToString("yyyy-MM-dd HH:mm:ss"), dtEnd.ToString("yyyy-MM-dd HH:mm:ss"));
        try
        {
            dsResult = GetData(sqlString1, out msg);
        }
        catch (Exception ex)
        {
            msg = ex.Message;
        }
        return dsResult.Tables[0];
    }

    /// <summary>
    /// 中控大屏成品率查询-新表取数据
    /// </summary>
    /// <param name="dtStart"></param>
    /// <param name="dtEnd"></param>
    /// <param name="strFactory"></param>
    /// <returns></returns>
    public DataTable GetCenterControlYieldData_AllData(DateTime dtStart, DateTime dtEnd)
    {
        DataSet dsResult = new DataSet();
        string msg = string.Empty;
        string sqlString1 = string.Format(@"
                select sum(cq.fpyqty01),
                    sum(cq.fpyqty02),
                    sum(cq.totalqty01),
                    sum(cq.totalqty02),
                    sum(cq.downgrade01),
                    sum(cq.downgrade02),
                    sum(cq.ng01),
                    sum(cq.ng02),
                    sum(cq.loss01),
                    sum(cq.loss02),
                    sum(cq.confirmok01),
                    sum(cq.confirmok02)
              from r_jkocenterscreen_quality cq
             where cq.datetime >= to_date('{0}', 'yyyy/mm/dd hh24:mi:ss')
               and cq.datetime <= to_date('{1}', 'yyyy/mm/dd hh24:mi:ss')",
               dtStart.ToString("yyyy/MM/dd HH:mm:ss"), dtEnd.ToString("yyyy/MM/dd HH:mm:ss"));
        try
        {
            dsResult = GetData(sqlString1, out msg);
        }
        catch (Exception ex)
        {
            msg = ex.Message;
        }
        return dsResult.Tables[0];
    }
    #endregion
}