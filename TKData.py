from datetime import datetime, timedelta, time

from .grpc.marketdata_pb2 import GetCandlesRequest, CandleInterval, GetCandlesResponse, HistoricCandle

from backtrader.feed import AbstractDataBase
from backtrader.utils.py3 import with_metaclass
from backtrader import TimeFrame, date2num

from BackTraderTinkoff import TKStore


class MetaTKData(AbstractDataBase.__class__):
    def __init__(self, name, bases, dct):
        super(MetaTKData, self).__init__(name, bases, dct)  # Инициализируем класс данных
        TKStore.DataCls = self  # Регистрируем класс данных в хранилище Alor


class TKData(with_metaclass(MetaTKData, AbstractDataBase)):
    """Данные Тинькофф"""
    params = (
        ('provider_name', None),  # Название провайдера. Если не задано, то первое название по ключу name
        ('four_price_doji', False),  # False - не пропускать дожи 4-х цен, True - пропускать
        ('live_bars', False),  # False - только история, True - история и новые бары
    )

    def islive(self):
        """Если подаем новые бары, то Cerebro не будет запускать preload и runonce, т.к. новые бары должны идти один за другим"""
        return self.p.live_bars

    def __init__(self, **kwargs):
        if self.p.timeframe == TimeFrame.Days:  # Дневной временной интервал (по умолчанию)
            self.interval = CandleInterval.CANDLE_INTERVAL_DAY
        elif self.p.timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            self.interval = CandleInterval.CANDLE_INTERVAL_WEEK
        elif self.p.timeframe == TimeFrame.Months:  # Месячный временной интервал
            self.interval = CandleInterval.CANDLE_INTERVAL_MONTH
        elif self.p.timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            if self.p.compression == 1:  # 1 минута
                self.interval = CandleInterval.CANDLE_INTERVAL_1_MIN
            elif self.p.compression == 2:  # 2 минуты
                self.interval = CandleInterval.CANDLE_INTERVAL_2_MIN
            elif self.p.compression == 3:  # 3 минуты
                self.interval = CandleInterval.CANDLE_INTERVAL_3_MIN
            elif self.p.compression == 5:  # 5 минут
                self.interval = CandleInterval.CANDLE_INTERVAL_5_MIN
            elif self.p.compression == 10:  # 10 минут
                self.interval = CandleInterval.CANDLE_INTERVAL_10_MIN
            elif self.p.compression == 15:  # 15 минут
                self.interval = CandleInterval.CANDLE_INTERVAL_15_MIN
            elif self.p.compression == 30:  # 30 минут
                self.interval = CandleInterval.CANDLE_INTERVAL_30_MIN
            elif self.p.compression == 60:  # 1 час
                self.interval = CandleInterval.CANDLE_INTERVAL_HOUR
            elif self.p.compression == 120:  # 2 часа
                self.interval = CandleInterval.CANDLE_INTERVAL_2_HOUR
            elif self.p.compression == 240:  # 4 часа
                self.interval = CandleInterval.CANDLE_INTERVAL_4_HOUR
        self.store = TKStore(**kwargs)  # Передаем параметры в хранилище Тинькофф. Может работать самостоятельно, не через хранилище
        self.provider_name = self.p.provider_name if self.p.provider_name else list(self.store.providers.keys())[0]  # Название провайдера, или первое название по ключу name
        self.account_id = str(self.store.providers[self.provider_name][0])  # Счет
        self.metadata = self.store.providers[self.provider_name][1]  # Токен
        self.figi = self.store.data_name_to_figi(self.p.dataname)  # По тикеру получаем код площадки и код тикера
        self.history_bars = []  # Исторические бары после применения фильтров
        self.subscribed = False  # Наличие подписки на получение новых баров
        self.live_mode = False  # Режим получения баров. False = История, True = Новые бары

    def setenvironment(self, env):
        """Добавление хранилища Тинькофф в cerebro"""
        super(TKData, self).setenvironment(env)
        env.addstore(self.store)  # Добавление хранилища Тинькофф в cerebro

    def start(self):
        super(TKData, self).start()
        self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) баров
        from_ = self.store.msk_datetime_to_timestamp(self.p.fromdate) if self.p.fromdate else None  # Дата и время начала выборки
        to = self.store.msk_datetime_to_timestamp(self.p.todate) if self.p.fromdate else None  # Дата и время окончания выборки
        request = GetCandlesRequest(instrument_id=self.figi, to=to, interval=self.interval)
        setattr(GetCandlesRequest, 'from', from_)  # Из-за ключевого слова from в Python придется обращаться к свойству
        response: GetCandlesResponse = self.store.stub_market_data.GetCandles.with_call(request=request, metadata=self.metadata)  # Получаем бары из Тинькофф
        for bar in response.candles:  # Пробегаемся по всем полученным барам
            if self.is_bar_valid(bar):  # Если исторический бар соответствует всем условиям выборки
                self.history_bars.append(bar)  # то добавляем бар
        if len(self.history_bars) > 0:  # Если был получен хотя бы 1 бар
            self.put_notification(self.CONNECTED)  # то отправляем уведомление о подключении и начале получения исторических баров

    def _load(self):
        """Загружаем бар из истории или новый бар в BackTrader"""
        if not self.p.live_bars:  # Если получаем только историю (self.historyBars)
            if len(self.history_bars) > 0:  # Если есть исторические данные
                bar = self.history_bars[0]  # Берем первый бар из выборки, с ним будем работать
                self.history_bars.remove(bar)  # Убираем его из хранилища новых баров
            else:  # Если исторических данных нет
                self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения исторических баров
                if not self.p.LiveBars:  # Если новые бары не принимаем
                    return False  # Больше сюда заходить не будем
                return None  # Будем заходить еще
        else:  # Если получаем историю и новые бары
            raise NotImplementedError  # TODO Тинькофф выдает подписки только на интервалах 1 и 5 минут. Нужно или группировать эти подписки в нужный интервал, или вводить расписание на бирже
        # Все проверки пройдены. Записываем полученный исторический/новый бар
        self.lines.datetime[0] = date2num(self.get_bar_open_date_time(bar))  # DateTime
        self.lines.open[0] = self.store.alor_to_bt_price(self.exchange, self.symbol, bar['open'])  # Open
        self.lines.high[0] = self.store.alor_to_bt_price(self.exchange, self.symbol, bar['high'])  # High
        self.lines.low[0] = self.store.alor_to_bt_price(self.exchange, self.symbol, bar['low'])  # Low
        self.lines.close[0] = self.store.alor_to_bt_price(self.exchange, self.symbol, bar['close'])  # Close
        self.lines.volume[0] = bar['volume']  # Volume
        self.lines.openinterest[0] = 0  # Открытый интерес в Алор не учитывается
        return True  # Будем заходить сюда еще

    # Функции

    def is_bar_valid(self, bar: HistoricCandle):
        """Проверка бара на соответствие условиям выборки"""
        dt_open = self.store.timestamp_to_msk_datetime(bar.time)  # Дата и время открытия бара
        if self.p.sessionstart != time.min and dt_open.time() < self.p.sessionstart:  # Если задано время начала сессии и открытие бара до этого времени
            return False  # то бар не соответствует условиям выборки
        dt_close = self.get_bar_close_date_time(dt_open)  # Дата и время закрытия бара
        if self.p.sessionend != time(23, 59, 59, 999990) and dt_close.time() > self.p.sessionend:  # Если задано время окончания сессии и закрытие бара после этого времени
            return False  # то бар не соответствует условиям выборки
        high = self.store.quotation_to_float(bar.high)  # High
        low = self.store.quotation_to_float(bar.low)  # Low
        if not self.p.four_price_doji and high == low:  # Если не пропускаем дожи 4-х цен, но такой бар пришел
            return False  # то бар не соответствует условиям выборки
        time_market_now = self.get_tinkoff_date_time_now()  # Текущее биржевое время
        if dt_close > time_market_now and time_market_now.time() < self.p.sessionend:  # Если время закрытия бара еще не наступило на бирже, и сессия еще не закончилась
            return False  # то бар не соответствует условиям выборки
        return True  # В остальных случаях бар соответствуем условиям выборки

    def get_bar_open_date_time(self, bar: HistoricCandle):
        """Дата и время открытия бара. Переводим из UTC в MSK для интрадея. Оставляем без времени для дневок и выше."""
        return self.store.utc_to_msk_date_time(bar.time)\
            if self.p.timeframe in (TimeFrame.Minutes, TimeFrame.Seconds)\
            else bar.time.date()  # Время открытия бара

    def get_bar_close_date_time(self, dt_open, period=1):
        """Дата и время закрытия бара"""
        if self.p.timeframe == TimeFrame.Days:  # Дневной временной интервал (по умолчанию)
            return dt_open + timedelta(days=period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return dt_open + timedelta(weeks=period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Months:  # Месячный временной интервал
            year = dt_open.year  # Год
            next_month = dt_open.month + period  # Добавляем месяцы
            if next_month > 12:  # Если произошло переполнение месяцев
                next_month -= 12  # то вычитаем год из месяцев
                year += 1  # ставим следующий год
            return datetime(year, next_month, 1)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Years:  # Годовой временной интервал
            return dt_open.replace(year=dt_open.year + period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            return dt_open + timedelta(minutes=self.p.compression * period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Seconds:  # Секундный временной интервал
            return dt_open + timedelta(seconds=self.p.compression * period)  # Время закрытия бара

    def get_tinkoff_date_time_now(self):
        """Текущая дата и время на сервере Тинькофф с учетом разницы (передается в подписках раз в 4 минуты)"""
        return datetime.now(self.store.tz_msk).replace(tzinfo=None) + self.store.delta
