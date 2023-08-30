from typing import Union  # Объединение типов
import collections

from grpc import ssl_channel_credentials, secure_channel, RpcError, StatusCode  # Защищенный канал
from .grpc.common_pb2 import MoneyValue, Quotation
from .grpc.instruments_pb2 import InstrumentRequest, InstrumentIdType, InstrumentResponse, Instrument
from .grpc.instruments_pb2_grpc import InstrumentsServiceStub
from .grpc.operations_pb2_grpc import OperationsServiceStub
from .grpc.orders_pb2_grpc import OrdersServiceStub, OrdersStreamServiceStub
from .grpc.stoporders_pb2_grpc import StopOrdersServiceStub
from .grpc.marketdata_pb2_grpc import MarketDataServiceStub

from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass


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
    """Хранилище Тинькофф. Работает с мультисчетами
    На основе Tinkoff Invest API https://tinkoff.github.io/investAPI/

    В параметр providers передавать список счетов в виде словаря с ключами:
    - provider_name - Название провайдера. Должно быть уникальным
    - account_id - Торговый счет
    - token - Торговый токен доступа из Config

    Пример использования:
    provider1 = dict(provider_name='tinkoff_trade', account_id=Config.AccountIds[0], token=Config.Token)  # Торговый счет Tinkoff
    provider2 = dict(provider_name='tinkoff_iia', account_id=Config.AccountIds[1], token=Config.Token)  # ИИС Tinkoff
    store = TKStore(providers=[provider1, provider2])  # Мультисчет
    """
    params = (
        ('providers', None),  # Список провайдеров счетов в виде словаря
    )
    server = 'invest-public-api.tinkoff.ru'  # Торговый сервер

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

    def __init__(self, **kwargs):
        super(TKStore, self).__init__()
        if 'providers' in kwargs:  # Если хранилище создаем из данных/брокера (не рекомендуется)
            self.p.providers = kwargs['providers']  # то список провайдеров берем из переданного ключа providers
        self.notifs = collections.deque()  # Уведомления хранилища
        self.providers = {}  # Справочник провайдеров
        self.channel = secure_channel(self.server, ssl_channel_credentials())  # Защищенный канал
        self.stub_instruments = InstrumentsServiceStub(self.channel)  # Сервис инструментов
        self.stub_market_data = MarketDataServiceStub(self.channel)  # Сервис исторических баров
        self.stub_operations = OperationsServiceStub(self.channel)  # Сервис операций
        self.stub_orders = OrdersServiceStub(self.channel)  # Сервис заявок
        self.stub_orders_stream = OrdersStreamServiceStub(self.channel)  # Сервис подписки на заявки
        self.stub_stop_orders = StopOrdersServiceStub(self.channel)  # Сервис стоп заявок
        for provider in self.p.providers:  # Пробегаемся по всем провайдерам
            provider_name = provider['provider_name'] if 'provider_name' in provider else 'default'  # Название провайдера или название по умолчанию
            metadata = [('authorization', f'Bearer {provider["token"]}')]  # Торговый токен доступа
            self.providers[provider_name] = (provider['account_id'], metadata)  # Работа с Tinkoff Invest API из Python https://tinkoff.github.io/investAPI/ с токеном по счету
        self.default_account_id = list(self.providers.values())[0][0]  # Счет по умолчанию (первый) для работы со справочниками
        self.default_metadata = list(self.providers.values())[0][1]  # Заголовок запроса по умолчанию (первый) для работы со справочниками
        self.symbols = {}  # Информация о тикерах
        self.new_bars = []  # Новые бары по всем подпискам на тикеры из Тинькофф

    def start(self):
        pass

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def stop(self):
        self.channel.close()  # Закрываем защищенный канал

    # Функции

    def get_symbol_info(self, class_code, symbol, reload=False) -> Union[Instrument, None]:
        """Получение информации тикера

        :param str class_code: : Код площадки
        :param str symbol: Тикер
        :param bool reload: Получить информацию с Тинькофф
        :return: Значение из кэша/Тинькоффили None, если тикер не найден
        """
        if reload or (class_code, symbol) not in self.symbols:  # Если нужно получить информацию с Тинькофф или нет информации о тикере в справочнике
            try:  # Пробуем
                request = InstrumentRequest(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER, class_code=class_code, id=symbol)  # Поиск тикера по коду площадки/названию
                response, call = self.stub_instruments.GetInstrumentBy.with_call(request=request, metadata=self.default_metadata)  # Получаем информацию о тикере
            except RpcError as ex:  # Если произошла ошибка
                if ex.args[0].code == StatusCode.NOT_FOUND:  # Тикер не найден
                    print(f'Информация о {class_code}.{symbol} не найдена')
                return None  # то возвращаем пустое значение
            self.symbols[(class_code, symbol)] = response.instrument  # Заносим информацию о тикере в справочник
        return self.symbols[(class_code, symbol)]  # Возвращаем значение из справочника

    def figi_to_symbol_info(self, figi):
        """Получение информации тикера по уникальному коду

        :param str figi: : Уникальный код тикера
        :return: Значение из кэша/Тинькофф или None, если тикер не найден
        """
        try:  # Пробуем
            return next(item for item in self.symbols.values() if item.figi == figi)  # вернуть значение из справочника
        except StopIteration:  # Если тикер не найден
            pass  # то продолжаем дальше
        try:  # Пробуем
            request = InstrumentRequest(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, class_code='', id=figi)  # Поиск тикера по уникальному коду
            response: InstrumentResponse
            response, call = self.stub_instruments.GetInstrumentBy.with_call(request=request, metadata=self.default_metadata)  # Получаем информацию о тикере
            instrument = response.instrument  # Информация о тикере
            self.symbols[(instrument.class_code, instrument.ticker)] = instrument  # Заносим информацию о тикере в справочник
            return instrument
        except RpcError as ex:  # Если произошла ошибка
            if ex.args[0].code == StatusCode.NOT_FOUND:  # Тикер не найден
                print(f'Информация о тикере с figi={figi} не найдена')
            return None  # то возвращаем пустое значение

    def data_name_to_class_code_symbol(self, dataname):
        """Биржа и код тикера из названия тикера. Если задается без биржи, то по умолчанию ставится MOEX

        :param str dataname: Название тикера
        :return: Код площадки и код тикера
        """
        symbol_parts = dataname.split('.')  # По разделителю пытаемся разбить тикер на части
        if len(symbol_parts) >= 2:  # Если тикер задан в формате <Код площадки>.<Код тикера>
            class_code = symbol_parts[0]  # Код площадки
            symbol = '.'.join(symbol_parts[1:])  # Код тикера
        else:  # Если тикер задан без площадки
            symbol = dataname  # Код тикера
            class_code = next(item.class_code for item in self.symbols if item.ticker == symbol)  # Получаем код площадки первого совпадающего тикера
        return class_code, symbol  # Возвращаем код площадки и код тикера

    @staticmethod
    def class_code_symbol_to_data_name(class_code, symbol):
        """Название тикера из кода площадки и кода тикера

        :param str class_code: Код площадки
        :param str symbol: Тикер
        :return: Название тикера
        """
        return f'{class_code}.{symbol}'

    @staticmethod
    def money_value_to_float(money_value: MoneyValue) -> float:
        """Перевод денежной суммы в валюте в вещественное число

        :param money_value: Денежная сумма в валюте
        :return: Вещественное число
        """
        return money_value.units + money_value.nano / 1_000_000_000

    @staticmethod
    def float_to_quotation(f: float) -> Quotation:
        """Перевод вещественного числа в денежную сумму

        :param float f: Вещественное число
        :return: Денежная сумма
        """
        int_f = int(f)  # Целая часть числа
        return Quotation(units=int_f, nano=int((f - int_f) * 1_000_000_000))

    @staticmethod
    def quotation_to_float(quotation: Quotation) -> float:
        """Перевод денежной суммы в вещественное число

        :param quotation: Денежная сумма
        :return: Вещественное число
        """
        return quotation.units + quotation.nano / 1_000_000_000




