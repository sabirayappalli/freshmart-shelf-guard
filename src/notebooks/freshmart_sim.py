"""FreshMart synthetic grocery simulator.

Generates a 80-store x 200-SKU regional grocery chain and simulates one year of
store-SKU daily demand, censored POS sales, inventory, replenishment orders and
deliveries under the chain's LEGACY min/max replenishment policy (a trailing
28-day average, blind to promotions and holidays) -- the root cause of stockouts.

The same `replay()` engine is reused by the backtest notebook to re-run the hold-out
window under the ML-driven policy against the *same* demand realisations, so the
comparison is apples-to-apples.

Everything here is synthetic. No customer data.
"""
from __future__ import annotations

import datetime as dt
import numpy as np
import pandas as pd

SEED = 42
N_STORES = 80
N_SKUS = 200
START = dt.date(2025, 11, 1)
END = dt.date(2026, 11, 1)          # last simulated day ("yesterday" for a 2026-11-02 run)
WARMUP_DAYS = 35

REGIONS = ["Northeast", "Southeast", "Midwest", "West"]
FORMATS = {"Supercenter": 1.6, "Neighborhood": 1.0, "Express": 0.5}
CITIES = {
    "Northeast": [("Boston", "MA"), ("Hartford", "CT"), ("Albany", "NY"), ("Providence", "RI"), ("Newark", "NJ")],
    "Southeast": [("Atlanta", "GA"), ("Charlotte", "NC"), ("Tampa", "FL"), ("Nashville", "TN"), ("Raleigh", "NC")],
    "Midwest": [("Chicago", "IL"), ("Columbus", "OH"), ("Indianapolis", "IN"), ("Milwaukee", "WI"), ("Detroit", "MI")],
    "West": [("Denver", "CO"), ("Phoenix", "AZ"), ("Sacramento", "CA"), ("Portland", "OR"), ("Salt Lake City", "UT")],
}

# category: (n_skus, base_daily_units(min,max), price(min,max), margin, perishable, lead_time(min,max), case_pack, shrink_rate, promo_elasticity)
CATEGORIES = {
    "Dairy":      (26, (6, 30), (1.99, 6.49), 0.28, True,  (1, 2), 12, 0.012, 2.2),
    "Produce":    (30, (5, 35), (0.99, 5.99), 0.35, True,  (1, 2), 10, 0.020, 2.0),
    "Bakery":     (18, (3, 18), (2.49, 7.99), 0.45, True,  (1, 1), 8,  0.030, 1.8),
    "Meat":       (18, (2, 14), (4.99, 18.99), 0.25, True, (1, 2), 6,  0.015, 2.6),
    "Frozen":     (22, (2, 12), (2.99, 9.99), 0.30, False, (3, 5), 12, 0.002, 2.4),
    "Beverages":  (28, (5, 28), (1.29, 8.99), 0.32, False, (3, 5), 24, 0.001, 3.0),
    "Snacks":     (26, (4, 22), (1.99, 5.99), 0.38, False, (3, 5), 18, 0.001, 3.2),
    "Pantry":     (20, (2, 14), (1.49, 8.49), 0.33, False, (4, 6), 12, 0.000, 2.0),
    "Household":  (12, (1, 8),  (3.99, 14.99), 0.36, False, (4, 6), 6, 0.000, 1.6),
}
SUBCATS = {
    "Dairy": ["Milk", "Yogurt", "Cheese", "Eggs", "Butter"],
    "Produce": ["Berries", "Bananas", "Leafy Greens", "Avocados", "Apples", "Tomatoes"],
    "Bakery": ["Bread", "Bagels", "Muffins", "Tortillas"],
    "Meat": ["Chicken", "Ground Beef", "Pork", "Deli"],
    "Frozen": ["Pizza", "Ice Cream", "Vegetables", "Meals"],
    "Beverages": ["Water", "Soda", "Juice", "Sports Drinks", "Coffee"],
    "Snacks": ["Chips", "Cookies", "Crackers", "Nuts"],
    "Pantry": ["Pasta", "Cereal", "Canned Goods", "Baking"],
    "Household": ["Paper Towels", "Detergent", "Cleaning"],
}
BRANDS = ["FreshMart", "Valley Farms", "Golden Acre", "Sunny Day", "Northstar", "Blue Ridge", "Harvest Co", "Peak"]

HOLIDAYS = {  # date: (name, demand multiplier, categories boosted ("*" = all))
    dt.date(2025, 11, 27): ("Thanksgiving", 0.55, "*"),
    dt.date(2025, 12, 25): ("Christmas", 0.40, "*"),
    dt.date(2026, 1, 1): ("New Year", 0.70, "*"),
    dt.date(2026, 2, 8): ("Super Bowl", 1.0, "*"),
    dt.date(2026, 5, 25): ("Memorial Day", 0.9, "*"),
    dt.date(2026, 7, 4): ("Independence Day", 0.8, "*"),
    dt.date(2026, 9, 7): ("Labor Day", 0.9, "*"),
}
# pre-holiday stock-up windows: (holiday, days_before, multiplier, categories)
PRE_HOLIDAY = [
    (dt.date(2025, 11, 27), 4, 1.9, {"Bakery", "Dairy", "Produce", "Pantry", "Meat"}),
    (dt.date(2025, 12, 25), 4, 1.7, {"Bakery", "Dairy", "Produce", "Meat", "Beverages"}),
    (dt.date(2026, 2, 8), 3, 1.8, {"Snacks", "Beverages", "Frozen", "Meat"}),
    (dt.date(2026, 5, 25), 3, 1.5, {"Meat", "Beverages", "Snacks", "Produce"}),
    (dt.date(2026, 7, 4), 3, 1.8, {"Meat", "Beverages", "Snacks", "Produce", "Bakery"}),
    (dt.date(2026, 9, 7), 3, 1.5, {"Meat", "Beverages", "Snacks"}),
]
DOW_MULT = np.array([0.88, 0.85, 0.90, 0.95, 1.12, 1.32, 1.18])  # Mon..Sun


def build_dimensions(rng: np.random.Generator):
    stores = []
    sid = 1001
    for region in REGIONS:
        for i in range(N_STORES // len(REGIONS)):
            city, state = CITIES[region][i % 5]
            fmt = rng.choice(list(FORMATS), p=[0.25, 0.55, 0.20])
            stores.append(dict(store_id=f"S{sid}", store_name=f"FreshMart {city} #{sid}", region=region,
                               store_format=fmt, city=city, state=state,
                               opened_year=int(rng.integers(1998, 2024)),
                               sqft=int({"Supercenter": 95000, "Neighborhood": 45000, "Express": 15000}[fmt] * rng.uniform(0.85, 1.15))))
            sid += 1
    stores = pd.DataFrame(stores)

    prods = []
    k = 1
    for cat, (n, (bmin, bmax), (pmin, pmax), margin, perish, (lmin, lmax), case, shrink, elast) in CATEGORIES.items():
        for j in range(n):
            sub = SUBCATS[cat][j % len(SUBCATS[cat])]
            price = round(float(rng.uniform(pmin, pmax)), 2)
            prods.append(dict(
                sku_id=f"SKU{k:05d}",
                sku_name=f"{rng.choice(BRANDS)} {sub} {['Classic','Organic','Family Size','Lite','Premium','Value'][j % 6]}",
                category=cat, subcategory=sub, unit_price=price,
                unit_cost=round(price * (1 - margin) * rng.uniform(0.92, 1.08), 2),
                perishable=perish, shelf_life_days=int(rng.integers(3, 15)) if perish else int(rng.integers(90, 720)),
                case_pack=case, supplier_id=f"SUP{(k % 23) + 1:03d}",
                lead_time_days=int(rng.integers(lmin, lmax + 1)),
                _base=float(np.exp(rng.uniform(np.log(bmin), np.log(bmax)))),
                _shrink=shrink, _elast=elast))
            k += 1
    products = pd.DataFrame(prods)
    return stores, products


def build_promotions(rng, products):
    promos = []
    pid = 1
    days = (END - START).days + 1
    for _, p in products.iterrows():
        n = int(rng.integers(3, 8))
        for _ in range(n):
            s = START + dt.timedelta(days=int(rng.integers(0, days - 7)))
            length = int(rng.choice([4, 7, 7, 10]))
            scope = rng.choice(["ALL"] + REGIONS, p=[0.4, 0.15, 0.15, 0.15, 0.15])
            promos.append(dict(promo_id=f"P{pid:06d}", sku_id=p.sku_id, region=scope,
                               start_date=s.isoformat(), end_date=(s + dt.timedelta(days=length - 1)).isoformat(),
                               discount_pct=float(rng.choice([0.10, 0.15, 0.20, 0.25, 0.30])),
                               promo_type=str(rng.choice(["TPR", "BOGO", "Circular", "Digital Coupon"]))))
            pid += 1
    # Seed a few promos that are live over the scoring window so the alert feed has a promo story.
    for sku in products.sample(25, random_state=7).sku_id:
        promos.append(dict(promo_id=f"P{pid:06d}", sku_id=sku, region=str(rng.choice(REGIONS)),
                           start_date="2026-10-30", end_date="2026-11-05", discount_pct=0.25,
                           promo_type="Circular"))
        pid += 1
    return pd.DataFrame(promos)


def demand_multipliers(stores, products, promos, dates):
    """Return (T, S, K) deterministic demand multiplier and (T, S, K) promo discount."""
    T, S, K = len(dates), len(stores), len(products)
    cats = products.category.values
    dow = np.array([DOW_MULT[d.weekday()] for d in dates])[:, None, None]
    mult = np.ones((T, S, K)) * dow

    doy = np.array([d.timetuple().tm_yday for d in dates])
    summer = 1 + 0.25 * np.sin((doy - 100) / 365 * 2 * np.pi)          # peaks ~July
    winter = 1 + 0.12 * np.cos((doy - 355) / 365 * 2 * np.pi)          # peaks ~Dec
    for k, c in enumerate(cats):
        if c in ("Beverages", "Produce"):
            mult[:, :, k] *= summer[:, None]
        if c in ("Pantry", "Bakery", "Dairy"):
            mult[:, :, k] *= winter[:, None]

    date_idx = {d: i for i, d in enumerate(dates)}
    for hd, (_, m, _) in HOLIDAYS.items():
        if hd in date_idx:
            mult[date_idx[hd]] *= m
    for hd, before, m, cset in PRE_HOLIDAY:
        kmask = np.isin(cats, list(cset))
        for b in range(1, before + 1):
            d = hd - dt.timedelta(days=b)
            if d in date_idx:
                mult[date_idx[d]][:, kmask] *= m * (1 - 0.12 * (b - 1))

    disc = np.zeros((T, S, K))
    sku_idx = {s: i for i, s in enumerate(products.sku_id)}
    reg = stores.region.values
    for _, pr in promos.iterrows():
        k = sku_idx[pr.sku_id]
        smask = np.ones(S, bool) if pr.region == "ALL" else (reg == pr.region)
        d0, d1 = dt.date.fromisoformat(pr.start_date), dt.date.fromisoformat(pr.end_date)
        for d in pd.date_range(d0, d1).date:
            if d in date_idx:
                disc[date_idx[d], smask, k] = np.maximum(disc[date_idx[d], smask, k], pr.discount_pct)
    elast = products._elast.values[None, None, :]
    mult *= 1 + elast * disc            # 25% off * 3.0 elasticity => +75% demand
    return mult, disc


def simulate_demand(rng, stores, products, mult):
    S, K = len(stores), len(products)
    fmt = stores.store_format.map(FORMATS).values
    affinity = rng.uniform(0.6, 1.4, size=(S, K))
    base = products._base.values[None, :] * fmt[:, None] * affinity      # (S, K)
    lam = mult * base[None]
    # regional weather / local-event shocks
    T = mult.shape[0]
    shock = rng.lognormal(0, 0.12, size=(T, S, 1))
    lam = lam * shock
    shape = 6.0                                                          # gamma-poisson overdispersion
    return rng.poisson(rng.gamma(shape, lam / shape)).astype(np.int32), base


def replay(demand, products, base, policy, init_on_hand=None, init_avg=None, pipeline_in=None,
           forecast_fn=None, rng=None, t_offset=0):
    """Run the store-SKU inventory simulation.

    policy: "legacy" (trailing average min/max) or "ml" (order-up-to from forecast_fn(t) -> (mu, sigma) arrays (S,K)
            for demand over lead_time+1 days).
    Returns dict of (T,S,K) arrays: sales, lost, on_hand_end, on_order, arrivals, shrink, orders
    """
    rng = rng or np.random.default_rng(SEED + 1)
    T, S, K = demand.shape
    L = products.lead_time_days.values.astype(int)
    case = products.case_pack.values
    shrink_rate = products._shrink.values
    review = 2
    avg = base.copy() if init_avg is None else init_avg.copy()
    reorder_pt = None
    on_hand = (np.ceil(base * (L + review + 1)) if init_on_hand is None else init_on_hand.copy()).astype(np.int64)
    max_l = int(L.max()) + 8
    pipe = np.zeros((max_l, S, K), np.int64) if pipeline_in is None else pipeline_in.copy()
    out = {k: np.zeros((T, S, K), np.int32) for k in ["sales", "lost", "on_hand_end", "on_order", "arrivals", "shrink", "orders",
                                                     "inbound_d1", "inbound_d2", "inbound_d3"]}
    for t in range(T):
        arrivals = pipe[0].copy()
        pipe = np.roll(pipe, -1, axis=0); pipe[-1] = 0
        on_hand += arrivals
        sales = np.minimum(demand[t], on_hand)
        lost = demand[t] - sales
        on_hand -= sales
        shrink = rng.binomial(on_hand, np.broadcast_to(shrink_rate, on_hand.shape))
        on_hand -= shrink
        avg = avg + (sales - avg) / 28.0                                  # legacy: trailing avg of *observed* sales
        on_order = pipe.sum(axis=0)
        position = on_hand + on_order
        if policy == "legacy":
            reorder_pt = avg * (L + 1) * 1.0
            up_to = reorder_pt + avg * review
            need = np.where(position < reorder_pt, up_to - position, 0)
        else:
            mu, sigma = forecast_fn(t + t_offset)
            up_to = mu + 1.65 * sigma                                     # ~95% cycle service level
            need = np.where(position < up_to, up_to - position, 0)
        q = (np.ceil(need / case) * case).astype(np.int64)
        fill = np.where(rng.random((S, K)) < 0.05, 0.6, 1.0)              # supplier short-ships 5% of POs
        q_recv = np.floor(q * fill).astype(np.int64)
        for k_l in np.unique(L):
            kmask = L == k_l
            pipe[k_l - 1][:, kmask] += q_recv[:, kmask]
        out["sales"][t], out["lost"][t], out["on_hand_end"][t] = sales, lost, on_hand
        out["on_order"][t], out["arrivals"][t], out["shrink"][t], out["orders"][t] = pipe.sum(0), arrivals, shrink, q
        out["inbound_d1"][t], out["inbound_d2"][t], out["inbound_d3"][t] = pipe[0], pipe[1], pipe[2]   # WMS open-PO due dates
    out["_state"] = dict(on_hand=on_hand, avg=avg, pipe=pipe)
    return out


def generate():
    rng = np.random.default_rng(SEED)
    stores, products = build_dimensions(rng)
    promos = build_promotions(rng, products)
    dates_all = [START - dt.timedelta(days=WARMUP_DAYS) + dt.timedelta(days=i) for i in range((END - START).days + 1 + WARMUP_DAYS)]
    mult, disc = demand_multipliers(stores, products, promos, dates_all)
    demand, base = simulate_demand(rng, stores, products, mult)
    sim = replay(demand, products, base, "legacy")
    w = WARMUP_DAYS
    dates = dates_all[w:]
    arrays = {k: v[w:] for k, v in sim.items() if not k.startswith("_")}
    return dict(stores=stores, products=products, promos=promos, dates=dates, demand=demand[w:], disc=disc[w:],
                base=base, sim=arrays, dates_all=dates_all, demand_all=demand, mult_all=mult)


def to_long(dates, stores, products, **arrays):
    T, S, K = next(iter(arrays.values())).shape
    idx_t, idx_s, idx_k = np.meshgrid(np.arange(T), np.arange(S), np.arange(K), indexing="ij")
    df = pd.DataFrame({"date": np.array(dates, dtype="datetime64[D]")[idx_t.ravel()],
                       "store_id": stores.store_id.values[idx_s.ravel()],
                       "sku_id": products.sku_id.values[idx_k.ravel()]})
    for k, v in arrays.items():
        df[k] = v.ravel()
    return df
