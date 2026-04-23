import requests, time, json, os, logging
from datetime import datetime, timedelta

logger = logging.getLogger("geocoder")

CACHE_FILE = os.path.join(os.path.dirname(__file__), "../../cache_enderecos.json")
CACHE_DIAS = 7

class NominatimGeocoder:
    BASE_URL = "https://nominatim.openstreetmap.org/reverse"
    ULTIMO_CHAMADO = 0.0

    def __init__(self, app_name="iatechhub-gts"):
        self.headers = {
            "User-Agent": f"{app_name}/1.0",
            "Accept-Language": "pt-BR,pt;q=0.9"
        }

    def _aguardar_rate_limit(self):
        agora = time.time()
        decorrido = agora - NominatimGeocoder.ULTIMO_CHAMADO
        if decorrido < 1.1:
            time.sleep(1.1 - decorrido)
        NominatimGeocoder.ULTIMO_CHAMADO = time.time()

    def buscar(self, lat, lon):
        self._aguardar_rate_limit()
        params = {"lat": lat, "lon": lon, "format": "json",
                  "addressdetails": 1, "zoom": 18}
        try:
            r = requests.get(self.BASE_URL, params=params,
                             headers=self.headers, timeout=8)
            r.raise_for_status()
            return self._formatar(r.json(), lat, lon)
        except Exception as e:
            logger.error(f"Erro geocoding {lat},{lon}: {e}")
            return {"sucesso": False, "endereco_curto": f"{lat:.4f},{lon:.4f}"}

    def _formatar(self, data, lat, lon):
        addr = data.get("address", {})
        rua     = addr.get("road") or addr.get("pedestrian") or addr.get("path") or ""
        numero  = addr.get("house_number", "")
        bairro  = addr.get("suburb") or addr.get("neighbourhood") or addr.get("district") or ""
        cidade  = addr.get("city") or addr.get("town") or addr.get("village") or addr.get("municipality") or ""
        estado  = addr.get("state", "")
        nome_local = data.get("name") or ""  # posto, empresa, etc

        UFS = {"Alagoas":"AL","Sergipe":"SE","Bahia":"BA","Pernambuco":"PE",
               "Sao Paulo":"SP","São Paulo":"SP","Parana":"PR","Paraná":"PR",
               "Rio de Janeiro":"RJ","Minas Gerais":"MG"}
        uf = UFS.get(estado, estado[:2].upper() if estado else "")

        partes = []
        if nome_local and nome_local != rua:
            partes.append(nome_local)
        if rua:
            partes.append(rua + (f", {numero}" if numero else ""))
        if bairro:
            partes.append(bairro)
        if cidade:
            partes.append(f"{cidade}/{uf}" if uf else cidade)

        curto = " — ".join(partes) if partes else data.get("display_name", f"{lat:.4f},{lon:.4f}")
        # Limita tamanho
        if len(curto) > 80:
            curto = curto[:77] + "..."

        return {
            "sucesso": True,
            "endereco_curto": curto,
            "endereco_completo": data.get("display_name", ""),
            "rua": rua,
            "numero": numero,
            "bairro": bairro,
            "cidade": cidade,
            "uf": uf,
            "consultado_em": datetime.now().isoformat()
        }


class CacheJSON:
    def __init__(self):
        self.arquivo = os.path.abspath(CACHE_FILE)
        self.dados = self._carregar()

    def _carregar(self):
        if os.path.exists(self.arquivo):
            try:
                with open(self.arquivo, "r", encoding="utf-8") as f:
                    dados = json.load(f)
                # Remove entradas antigas
                limite = (datetime.now() - timedelta(days=CACHE_DIAS)).isoformat()
                dados = {k: v for k, v in dados.items()
                         if v.get("consultado_em", "9999") > limite}
                return dados
            except:
                return {}
        return {}

    def _salvar(self):
        try:
            with open(self.arquivo, "w", encoding="utf-8") as f:
                json.dump(self.dados, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Erro ao salvar cache: {e}")

    def chave(self, lat, lon):
        return f"{round(float(lat), 4)}:{round(float(lon), 4)}"

    def get(self, lat, lon):
        return self.dados.get(self.chave(lat, lon))

    def set(self, lat, lon, endereco):
        self.dados[self.chave(lat, lon)] = endereco
        self._salvar()


class GeocoderGTS:
    """Classe principal — use esta no projeto."""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.nominatim = NominatimGeocoder("iatechhub-gts")
            cls._instance.cache = CacheJSON()
        return cls._instance

    def get_endereco(self, lat, lon):
        """Retorna endereco legivel para as coordenadas. Usa cache automaticamente."""
        if not lat or not lon:
            return None
        cached = self.cache.get(lat, lon)
        if cached:
            return cached
        resultado = self.nominatim.buscar(lat, lon)
        if resultado.get("sucesso"):
            self.cache.set(lat, lon, resultado)
        return resultado

    def get_curto(self, lat, lon):
        """Retorna so o endereco curto ou None."""
        r = self.get_endereco(lat, lon)
        return r.get("endereco_curto") if r else None


# Instancia global
geocoder = GeocoderGTS()
