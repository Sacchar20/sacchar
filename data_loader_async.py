import asyncio
import pandas as pd
from binance import AsyncClient
import pyarrow

API_KEY = ""
API_SECRET = ""


async def fetch_klines(client, symbol, interval, start_str, end_str, retries=3):
    for attempt in range(1, retries + 1):
        try:
            klines = await client.get_historical_klines(symbol, interval, start_str, end_str)
            print(f"✅ {symbol}: получено {len(klines)} свечей")
            return symbol, klines
        except Exception as e:
            print(f"❌ [{symbol}] Ошибка (попытка {attempt}/{retries}): {e}")
            if attempt < retries:
                await asyncio.sleep(10 * attempt)
            else:
                print(f"⚠️ [{symbol}] Все попытки исчерпаны. Пропускаем!.")
                return symbol, []


async def main():
    client = await AsyncClient.create(API_KEY, API_SECRET)

    try:
        # Получаем список всех пар с котировкой в BTC
        exchange_info = await client.get_exchange_info()
        btc_pairs = [s['symbol'] for s in exchange_info['symbols'] if s['quoteAsset'] == 'BTC']
        print(f"🔍 Найдено {len(btc_pairs)} BTC-пар")

        # Настройки
        start_str = "1 Feb 2025"
        end_str = "1 Mar 2025"
        interval = AsyncClient.KLINE_INTERVAL_1MINUTE

        # Параметры батча
        batch_size = 5
        delay_between_batches = 60  # секунд

        historical_data = {}

        # Разбиваем все пары на батчи по 5
        for i in range(0, len(btc_pairs), batch_size):
            batch = btc_pairs[i:i + batch_size]
            print(f"\n📦 Обрабатываем пары: {batch}")

            tasks = [
                fetch_klines(client, symbol, interval, start_str, end_str)
                for symbol in batch
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, Exception):
                    print(f"🛑 Необработанная ошибка задачи: {res}")
                    continue

                symbol, klines = res
                if not klines:
                    print(f"⚠️ Пропускаем {symbol} — 0 свечей")
                    continue

                historical_data[symbol] = klines

            if i + batch_size < len(btc_pairs):
                print(f"⏳ Ожидание {delay_between_batches} секунд перед следующей партией...")
                await asyncio.sleep(delay_between_batches)

        return historical_data

    finally:
        await client.close_connection()

if __name__ == "__main__":
    # Запускаем асинхронную функцию main() и получаем данные
    historical_data = asyncio.run(main())

    # 1. Вычисляем суммарный объём торгов для каждой пары за указанный период.
    # Индекс 5 в каждом kline содержит объём торгов (volume)
    symbol_volume = {}
    for symbol, klines in historical_data.items():
        total_volume = sum(float(kline[5]) for kline in klines) if klines else 0
        symbol_volume[symbol] = total_volume

    # 2. Сортируем пары по объёму в порядке убывания и выбираем топ-100
    sorted_symbols = sorted(symbol_volume.items(), key=lambda x: x[1], reverse=True)
    top_100_symbols = [symbol for symbol, volume in sorted_symbols[:100]]

    print("\nТоп-100 пар, отсортированных по суммарному объёму торгов за февраль 2025:")
    print(top_100_symbols)

    # 3. Формируем список записей только для топ-100 пар

    records = []
    for symbol in top_100_symbols:
        # Получаем исторические данные для текущей пары (если они есть)
        klines = historical_data.get(symbol, [])

        for kline in klines:
            record = {
                'symbol': symbol,
                'open_time': int(kline[0]),
                'open': float(kline[1]),
                'high': float(kline[2]),
                'low': float(kline[3]),
                'close': float(kline[4]),
                'volume': float(kline[5]),
                'close_time': int(kline[6]),
                'quote_asset_volume': float(kline[7]),
                'number_of_trades': int(kline[8]),
                'taker_buy_base_asset_volume': float(kline[9]),
                'taker_buy_quote_asset_volume': float(kline[10])
            }
            records.append(record)

    # 4. Создаем DataFrame из списка записей
    df = pd.DataFrame(records)

    # 5. Сохраняем DataFrame в файл Parquet
    df.to_parquet("historical_data.parquet", engine="pyarrow", compression="snappy")

    print("Данные успешно сохранены в файл historical_data.parquet")
