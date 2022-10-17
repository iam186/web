using Driver;
using OMS.SERV.ORM;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OMS.Position
{
    public struct Prx
    {
        public double fee;
        public double tax;
        public double costPx;
        public double avgPx;
    }
    class PositionListener : ModuleDriver
    {

        OMS_TEST omsds = null;
        ConcurrentDictionary<string, TaxFee> taxs = new ConcurrentDictionary<string, TaxFee>();
        ConcurrentDictionary<string, TradingFee> fees = new ConcurrentDictionary<string, TradingFee>();

        public override void prepareStart()
        {
            try
            {
                omsds = getComponent<OMS_TEST>();
                DaySettle();
                updPosition();
            }
            catch (Exception e)
            {
                log.Error(e);
            }
        }
        public override void OnZmqMessge(string strMsgr, string ip, int port, string topic)
        {
            try
            {
                QuickFix.Message qfx = null;
                try { qfx = new QuickFix.Message(strMsgr, false); } catch (Exception) { }
                if (qfx == null) return;
                if (!qfx.Header.IsSetField(35)) return;
                if (!qfx.IsSetField(18601)) return;
                if (!qfx.IsSetField(14)) return;
                if (!qfx.Header.GetField(35).Equals("8")) return;
                if (qfx.GetInt(14) == 0) return;

                string usr = qfx.GetField(18601);
                decimal cumQty = qfx.GetDecimal(14);
                decimal avgPx = qfx.GetDecimal(6);
                string bkr = null;
                if (bkr == null && qfx.IsSetField(76)) bkr = qfx.GetField(76);
                if (bkr == null && qfx.IsSetField(375)) bkr = qfx.GetField(375);
                if (bkr == null && qfx.IsSetField(128)) bkr = qfx.GetField(128);
                string acc = null;
                if (acc == null && qfx.IsSetField(1)) acc = qfx.GetField(1);
                string sub = null;
                if (sub == null && qfx.IsSetField(79)) sub = qfx.GetField(79);
                if (sub == null && qfx.IsSetField(109)) sub = qfx.GetField(109);
                if (sub == null && qfx.IsSetField(440)) sub = qfx.GetField(440);
                string side = null;
                if (side == null && qfx.IsSetField(54)) side = qfx.GetField(54);
                string sym = null;
                if (sym == null && qfx.IsSetField(55)) sym = qfx.GetField(55);
                updPosition(bkr, acc, sub, sym, side);
                qfx = null;
            }
            catch (Exception e)
            {
                log.Error(e);
            }

        }

        public void updPosition(string bkr = null, string acc = null, string sub = null, string sym = null, string side = null)
        {
            try
            {
                if (omsds == null) return;
                List<OMS.SERV.ORM.OrdCumQtyView> cv = null;
                if (bkr != null && acc != null && sub != null && side != null && sym != null)
                {
                    Dictionary<string, object> c = new Dictionary<string, object>();
                    c["ExecBroker"] = bkr;
                    c["Account"] = acc;
                    c["AllocAccount"] = sub;
                    c["Symbol"] = sym;
                    c["Side"] = side;
                    cv = omsds.Select<OMS.SERV.ORM.OrdCumQtyView>(c);
                }
                if (bkr == null && acc == null && sub == null && side == null && sym == null) cv = omsds.Select<OMS.SERV.ORM.OrdCumQtyView>();
                if (cv != null)
                {
                    foreach (OrdCumQtyView ocv in cv)
                    {
                        List<OrdCumQtyView> ol = new List<OrdCumQtyView>();
                        if (ocv.Symbol1 != null)
                        {
                            OrdCumQtyView ov1 = new OrdCumQtyView();
                            ov1.Insert(ocv);
                            ov1.AvgPx = ov1.AvgPx1;
                            ov1.Symbol = ov1.Symbol1;
                            ov1.Side = "1";
                            ol.Add(ov1);
                        }
                        if (ocv.Symbol2 != null)
                        {
                            OrdCumQtyView ov2 = new OrdCumQtyView();
                            ov2.Insert(ocv);
                            ov2.Side = "2";
                            ov2.AvgPx = ov2.AvgPx2;
                            ov2.Symbol = ov2.Symbol2;
                            ov2.CumQty = -ov2.CumQty;
                            ol.Add(ov2);
                        }
                        if (ol.Count == 0) ol.Add(ocv);
                        foreach (OrdCumQtyView ov in ol)
                        {
                            Dictionary<string, object> pm = new Dictionary<string, object>();
                            pm["Broker"] = ov.ExecBroker;
                            pm["Account"] = ov.Account;
                            pm["SubAccount"] = ov.AllocAccount;
                            pm["Symbol"] = ov.Symbol;
                            List<OMS.SERV.ORM.Position> pl = omsds.Select<OMS.SERV.ORM.Position>(pm);
                            OMS.SERV.ORM.Position pp = new OMS.SERV.ORM.Position();
                            pp.Broker = ov.ExecBroker;
                            pp.Account = ov.Account;
                            pp.SubAccount = ov.AllocAccount;
                            pp.Symbol = ov.Symbol;
                            if (pl.Count == 0) SetPosition(ov, pp);
                            foreach (OMS.SERV.ORM.Position p in pl) SetPosition(ov, p);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                log.Error(e);
            }
        }

        private void SetPosition(OMS.SERV.ORM.OrdCumQtyView ov, OMS.SERV.ORM.Position ps)
        {
            try
            {
                double ocum = ps.CumQty;
                ps.SysTime = DateTime.Now;
                ps.AccID = ov.AccID;
                ps.AccName = ov.AccName;
                ps.Account = ov.Account;
                ps.SubAccount = ov.AllocAccount;
                ps.AppUserID = ov.AppUserID;
                ps.SecurityType = ov.BondType;
                ps.SecurityDesc = ov.SecurityDesc;
                ps.Broker = ov.ExecBroker;
                if (ov.Side.Equals("1"))
                {
                    ps.TodayBuy = ov.CumQty;
                    ps.TodayBuyAvg = ov.AvgPx;
                }
                if (ov.Side.Equals("2"))
                {
                    ps.TodaySell = ov.CumQty;
                    ps.TodaySellAvg = ov.AvgPx;
                }
                ps.CumQty = ps.YesterdayCumQty + ps.TodayBuy - ps.TodaySell;
                try { ps.AvgPx = ((ps.YesterdayAvgPx * ps.YesterdayCumQty) + (ps.TodayBuyAvg * ps.TodayBuy) - (ps.TodaySell * ps.TodaySellAvg)) / (ps.YesterdayCumQty + ps.TodayBuy - ps.TodaySell); } catch (Exception) { ps.AvgPx = 0.0; }
                if (ps.AvgPx.Equals(double.NaN)) ps.AvgPx = 0;
                ps.Text = ov.ToJson().ToString();
                if (ps.CumQty != ocum)
                {
                    ps = omsds.InsertOrUpdate<OMS.SERV.ORM.Position>(ps);
                    if (ps != null)
                    {
                        PositionLog log = new PositionLog();
                        log.Insert(ps);
                        omsds.Insert<OMS.SERV.ORM.PositionLog>(log);
                        ps.Text = "";
                        ZmqSend(ps.ToJson().ToString());
                    }
                }
            }
            catch (Exception e)
            {
                log.Error(e);
            }
        }

        public void DaySettle()
        {
            try
            {
                if (omsds == null) return;
                TimeSpan t1 = new TimeSpan(8, 25, 0);
                TimeSpan t2 = new TimeSpan(13, 50, 0);
                TimeSpan t3 = new TimeSpan(14, 55, 0);
                TimeSpan t4 = new TimeSpan(18, 30, 0);
                bool b1 = DateTime.Now.TimeOfDay < t1;//開盤前
                bool b2 = DateTime.Now.TimeOfDay > t2 && DateTime.Now.TimeOfDay < t3;//盤後盤前
                bool b3 = DateTime.Now.TimeOfDay > t4;//下班後
                if (!(b1 && b2 && b3)) return;
                foreach (OMS.SERV.ORM.Position po in omsds.Select<OMS.SERV.ORM.Position>())
                {
                    if ((po.TodayBuy > 0 || po.TodaySell > 0) && po.Today != SqlDateTime.MinValue.Value)
                    {
                        po.YesterdayCumQty = po.YesterdayCumQty + po.TodayBuy - po.TodaySell;
                        try { po.YesterdayAvgPx = ((po.YesterdayAvgPx * po.YesterdayCumQty) + (po.TodayBuyAvg * po.TodayBuy) - (po.TodaySell * po.TodaySellAvg)) / (po.YesterdayCumQty + po.TodayBuy - po.TodaySell); } catch (Exception) { po.YesterdayAvgPx = 0.0; }
                        if (po.YesterdayAvgPx.Equals(double.NaN)) po.YesterdayAvgPx = 0;
                        po.TodayBuy = 0;
                        po.TodaySell = 0;
                        po.TodayBuyAvg = 0;
                        po.TodaySellAvg = 0;
                        po.Yesterday = po.Today;
                        po.Today = SqlDateTime.MinValue.Value;
                        po.Text = "日結";
                        PositionLog log = new PositionLog();
                        log.Insert(po);
                        omsds.Insert<OMS.SERV.ORM.PositionLog>(log);
                        omsds.Update(po);
                    }
                }
            }
            catch (Exception e)
            {
                log.Error(e);
            }
        }

        public Prx GetPx(OMS.SERV.ORM.Position pos)
        {
            Prx px = new Prx();
            Dictionary<string, object> p = new Dictionary<string, object>();
            p[""] = "";
            TaxFee tx = omsds.GetDefaultFrist<TaxFee>(p);
            p.Clear();
            TradingFee fe = omsds.GetDefaultFrist<TradingFee>(p);
            double amt = 0;
            double avgPx = 0;
            double total = pos.YesterdayCumQty + pos.TodayBuy - pos.TodaySell;
            try { amt = ((pos.YesterdayAvgPx * pos.YesterdayCumQty) + (pos.TodayBuyAvg * pos.TodayBuy) - (pos.TodaySell * pos.TodaySellAvg)); } catch (Exception) { }
            try { avgPx = amt / total; }catch (Exception) { }
            if (amt.Equals(double.NaN))     amt = 0;
            if (avgPx.Equals(double.NaN)) avgPx = 0;

            try
            {
                if (pos.SecurityType != null)
                {
                    if (tx != null)
                    {

                        if (pos.TodaySell > 0)
                        {
                            if (pos.SecurityType.IndexOf("期") < 0)
                            {
                                px.tax = amt * tx.Tax;
                            }
                            if (pos.SecurityType.Trim().ToUpper().Equals("CB")) px.tax = 0;
                        }
                    }
                    if (fe != null)
                    {
                        if (pos.SecurityType.IndexOf("期") < 0)
                        {
                            px.fee = amt * fe.Fee;
                            px.costPx = amt/
                        }
                    }
                }
            }
            catch (Exception e)
            {
                log.Error(e);
            }
            return px;
        }

        public void kill()
        {
            try
            {
                System.Diagnostics.Process[] currProcess = System.Diagnostics.Process.GetProcessesByName(System.Diagnostics.Process.GetCurrentProcess().ProcessName);
                foreach (System.Diagnostics.Process porc in currProcess)
                {
                    try { porc.Kill(); } catch (Exception) { }
                }
            }
            catch (Exception e)
            {
                log.Error(e);
            }
        }

        public override void prepareStop() { }
        public override void Start() { }
        public override void Stop() { }

    }
}
