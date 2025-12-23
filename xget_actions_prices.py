"""
Get historical data from Yahoo Finance for a list of symbols:
- dividends (optional, via Ticker.dividends)
- splits (optional, via Ticker.splits)
- capital gains (optional, via Ticker.capital_gains)   <-- added
- OHLC + Adj Close + Volume (optional, via yf.download; actions=True optional)

Install:
  pip install yfinance pandas
"""

import os
import sys
import pandas as pd
import yfinance as yf


def _series_to_df(s, colname):
    """Convert yfinance Series (date-indexed) to a clean DataFrame."""
    if s is None or len(s) == 0:
        return pd.DataFrame(columns=[colname], index=pd.DatetimeIndex([], name="date"))

    df = s.rename(colname).to_frame()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df.index.name = "date"
    return df.sort_index()


def _filter_date_range(df, start, end):
    if start is not None:
        df = df.loc[pd.to_datetime(start) :]
    if end is not None:
        df = df.loc[: pd.to_datetime(end)]
    return df


def _safe_mkdir(path):
    if path and not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def _out_path(out_dir, filename):
    return os.path.join(out_dir, filename) if out_dir else filename


def _standardize_price_columns(df_px, symbol):
    """
    Reduce single-symbol MultiIndex columns from yfinance.download to single-level columns.
    """
    if df_px is None or len(df_px) == 0:
        return df_px

    if not isinstance(df_px.columns, pd.MultiIndex):
        return df_px

    try:
        lv0 = df_px.columns.get_level_values(0)
        lv1 = df_px.columns.get_level_values(1)
    except Exception:
        df_px = df_px.copy()
        df_px.columns = ["|".join(map(str, c)) for c in df_px.columns.to_list()]
        return df_px

    if symbol in set(lv0):
        try:
            sub = df_px[symbol]  # ("SPY","Open") style
            if isinstance(sub, pd.DataFrame):
                return sub
        except Exception:
            pass

    if symbol in set(lv1):
        try:
            sub = df_px.xs(symbol, axis=1, level=1, drop_level=True)  # ("Open","SPY") style
            if isinstance(sub, pd.DataFrame):
                return sub
        except Exception:
            pass

    u0 = pd.Index(lv0).unique()
    u1 = pd.Index(lv1).unique()

    df_px = df_px.copy()
    if len(u0) == 1 and len(u1) > 1:
        df_px.columns = lv1
        return df_px
    if len(u1) == 1 and len(u0) > 1:
        df_px.columns = lv0
        return df_px

    df_px.columns = ["|".join(map(str, c)) for c in df_px.columns.to_list()]
    return df_px


def main():
    # -----------------------------
    # user parameters / toggles
    # -----------------------------
    symbols = ["SPY", "QQQ", "NVDA", "FTABX"]
    start = "2010-01-01"      # set to None for no lower bound
    end = None               # e.g. "2025-12-31" or None for no upper bound

    get_dividends = True
    get_splits = True
    get_capital_gains = True   # <--- added
    get_prices = True

    price_interval = "1d"    # e.g. "1d", "1wk", "1mo"
    prices_actions = True    # keep this as a separate toggle if you want
    write_csv = True
    out_dir = "."            # e.g. r"c:\data" or "."
    # -----------------------------

    if not (get_dividends or get_splits or get_capital_gains or get_prices):
        print("nothing to do: all toggles are False")
        return

    _safe_mkdir(out_dir)

    for symbol in symbols:
        symbol = str(symbol).upper().strip()
        if not symbol:
            continue

        print("=" * 70)
        print(f"symbol: {symbol}")
        print(f"date range filter: start={start} end={end}")
        print()

        # dividends
        if get_dividends:
            try:
                tkr = yf.Ticker(symbol)
                div = tkr.dividends
                df_div = _series_to_df(div, "dividend")
                df_div = _filter_date_range(df_div, start, end)
                if len(df_div) > 0:
                    df_div = df_div[df_div["dividend"] != 0]
            except Exception as e:
                print(f"error: failed to fetch dividends for {symbol}: {e}")
                df_div = pd.DataFrame(columns=["dividend"], index=pd.DatetimeIndex([], name="date"))

            if len(df_div) == 0:
                print("dividends: none")
            else:
                total = float(df_div["dividend"].sum())
                print("dividends:")
                print(f"  rows: {len(df_div)}")
                print(f"  from: {df_div.index.min().date().isoformat()}")
                print(f"  to:   {df_div.index.max().date().isoformat()}")
                print(f"  sum:  {total:.6f}")
                print(df_div.tail(20).to_string())
                if write_csv:
                    out_csv = _out_path(out_dir, f"{symbol}_dividends.csv")
                    df_div.to_csv(out_csv, float_format="%.10g")
                    print(f"  wrote: {out_csv}")
            print()

        # splits
        if get_splits:
            try:
                tkr = yf.Ticker(symbol)
                spl = tkr.splits
                df_spl = _series_to_df(spl, "split_ratio")
                df_spl = _filter_date_range(df_spl, start, end)
                if len(df_spl) > 0:
                    df_spl = df_spl[df_spl["split_ratio"] != 0]
            except Exception as e:
                print(f"error: failed to fetch splits for {symbol}: {e}")
                df_spl = pd.DataFrame(columns=["split_ratio"], index=pd.DatetimeIndex([], name="date"))

            if len(df_spl) == 0:
                print("splits: none")
            else:
                print("splits:")
                print(f"  rows: {len(df_spl)}")
                print(f"  from: {df_spl.index.min().date().isoformat()}")
                print(f"  to:   {df_spl.index.max().date().isoformat()}")
                print(df_spl.to_string())
                if write_csv:
                    out_csv = _out_path(out_dir, f"{symbol}_splits.csv")
                    df_spl.to_csv(out_csv, float_format="%.10g")
                    print(f"  wrote: {out_csv}")
            print()

        # capital gains (separate, like dividends/splits)
        if get_capital_gains:
            try:
                tkr = yf.Ticker(symbol)
                # Some tickers (often mutual funds/ETFs) can have capital gains distributions.
                # Many tickers will return an empty series.
                cg = tkr.capital_gains
                df_cg = _series_to_df(cg, "capital_gains")
                df_cg = _filter_date_range(df_cg, start, end)
                if len(df_cg) > 0:
                    df_cg = df_cg[df_cg["capital_gains"] != 0]
            except Exception as e:
                print(f"error: failed to fetch capital gains for {symbol}: {e}")
                df_cg = pd.DataFrame(columns=["capital_gains"], index=pd.DatetimeIndex([], name="date"))

            if len(df_cg) == 0:
                print("capital gains: none")
            else:
                total = float(df_cg["capital_gains"].sum())
                print("capital gains:")
                print(f"  rows: {len(df_cg)}")
                print(f"  from: {df_cg.index.min().date().isoformat()}")
                print(f"  to:   {df_cg.index.max().date().isoformat()}")
                print(f"  sum:  {total:.6f}")
                print(df_cg.to_string())
                if write_csv:
                    out_csv = _out_path(out_dir, f"{symbol}_capital_gains.csv")
                    df_cg.to_csv(out_csv, float_format="%.10g")
                    print(f"  wrote: {out_csv}")
            print()

        # prices (optionally include actions columns)
        if get_prices:
            try:
                df_px = yf.download(
                    symbol,
                    start=start,
                    end=end,
                    interval=price_interval,
                    auto_adjust=False,
                    actions=prices_actions,
                    progress=False,
                )
            except Exception as e:
                print(f"error: failed to fetch prices for {symbol}: {e}")
                df_px = pd.DataFrame()

            if df_px is None or len(df_px) == 0:
                print("prices: none")
            else:
                df_px = df_px.copy()
                df_px.index = pd.to_datetime(df_px.index).tz_localize(None)
                df_px.index.name = "date"

                df_px = _standardize_price_columns(df_px, symbol)

                preferred = [
                    "Open", "High", "Low", "Close", "Adj Close", "Volume",
                    "Dividends", "Stock Splits", "Capital Gains",
                ]
                keep = [c for c in preferred if c in df_px.columns]
                other = [c for c in df_px.columns if c not in keep]
                df_px = df_px[keep + other]

                print("prices:")
                print(f"  rows: {len(df_px)}")
                print(f"  from: {df_px.index.min().date().isoformat()}")
                print(f"  to:   {df_px.index.max().date().isoformat()}")
                print(df_px.tail(5).to_string())

                if write_csv:
                    suffix = "_actions" if prices_actions else ""
                    out_csv = _out_path(out_dir, f"{symbol}_prices_{price_interval}{suffix}.csv")
                    df_px.to_csv(out_csv, float_format="%.10g")
                    print(f"  wrote: {out_csv}")
            print()

    print("=" * 70)
    print("done")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("interrupted")
        sys.exit(130)
