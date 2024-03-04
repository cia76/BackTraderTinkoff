from collections import deque
from datetime import datetime
from threading import Thread
import logging

from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass

from TinkoffPy import TinkoffPy
from TinkoffPy.grpc.marketdata_pb2 import Candle, SubscriptionInterval


class MetaSingleton(MetaParams):
    """Метакласс для создания Singleton классов"""
    def __init__(cls, *args, **kwargs):
        """Инициализация класса"""
        super(MetaSingleton, cls).__init__(*args, **kwargs)
        cls._singleton = None  # Экземпляра класса еще нет

    def __call__(cls, *args, **kwargs):
        """Вызов класса"""
        if cls._singleton is None:  # Если класса нет в экземплярах класса
            cls._singleton = super(MetaSingleton, cls).__call__(*args, **kwargs)  # то создаем зкземпляр класса
        return cls._singleton  # Возвращаем экземпляр класса


class TKStore(with_metaclass(MetaSingleton, object)):
    """Хранилище Тинькофф"""
    logger = logging.getLogger('TKStore')  # Будем вести лог

    BrokerCls = None  # Класс брокера будет задан из брокера
    DataCls = None  # Класс данных будет задан из данных

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Возвращает новый экземпляр класса данных с заданными параметрами"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Возвращает новый экземпляр класса брокера с заданными параметрами"""
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self, provider=TinkoffPy()):
        super(TKStore, self).__init__()
        self.notifs = deque()  # Уведомления хранилища
        self.provider = provider  # Подключаемся ко всем торговым счетам
        self.new_bars = []  # Новые бары по всем подпискам на тикеры из Тинькофф

    def start(self):
        self.provider.on_candle = self.on_candle   # Обработчик новых баров по подписке из Тинькофф
        Thread(target=self.provider.subscriptions_marketdata_handler, name='SubscriptionsMarketdataThread').start()  # Создаем и запускаем поток обработки подписок на биржевую информацию

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def stop(self):
        self.provider.on_candle = self.provider.default_handler  # Возвращаем обработчик по умолчанию
        self.provider.close_channel()  # Закрываем канал перед выходом

    def on_candle(self, candle: Candle):
        """Обработка прихода нового бара"""
        tf = 'M1' if candle.interval == SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE else\
            'M5' if candle.interval == SubscriptionInterval.SUBSCRIPTION_INTERVAL_FIVE_MINUTES else None  # Т.к. для баров и подписок используются разные временнЫе интервалы, то используем временнОй интервал из расписания
        bar = dict(datetime=self.provider.utc_to_msk_datetime(datetime.utcfromtimestamp(candle.time.seconds)),  # Дату/время переводим из UTC в МСК
                   open=self.provider.quotation_to_float(candle.open),
                   high=self.provider.quotation_to_float(candle.high),
                   low=self.provider.quotation_to_float(candle.low),
                   close=self.provider.quotation_to_float(candle.close),
                   volume=int(candle.volume))
        self.new_bars.append(dict(guid=(candle.figi, tf), data=bar))
