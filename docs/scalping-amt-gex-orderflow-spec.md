# Sistema de Scalping AMT + GEX + Orderflow

Data: 2026-06-24
Status: especificacao inicial para pesquisa, backtest, paper trading e Binance Futures Demo.
Escopo: BTCUSDT, scalping intraday, capital logico inicial de R$100 em paper.

Aviso: isto nao e recomendacao financeira. E plano tecnico para testar hipoteses. Edge so existe depois de coleta, replay, backtest sem lookahead, paper e analise estatistica.

## 1. Tese central

Sistema automatiza leitura do trader citado:

- AMT/TPO define mapa: onde mercado aceita valor, rejeita valor, equilibra, desequilibra.
- GEX define regime: market maker amortece movimento em gamma positivo; amplifica movimento em gamma negativo.
- Orderflow define gatilho: agressao, absorcao, exaustao, CVD, book/depth.
- Liquidez define alvo: lows/highs, round numbers, nPOC, HVN/LVN, zonas de GEX, resting liquidity.
- Execucao vira mecanica: sem "acho", sem candle pattern solto, tudo scoreado e registrado.

Regra-mestra:

```text
Contexto macro (AMT/TPO/GEX) -> zona operacional -> fluxo confirma -> ordem pequena -> stop mecanico -> alvo por liquidez/R.
```

## Sintese e analise dos materiais enviados

### Material 1: scalp BTC 3R

Sequencia operacional real:

1. Contexto: preco em regiao de volume low/LVN, perto de low e liquidez abaixo.
2. Regime: GEX negativo. Hipotese: se low rompe, dealer precisa vender junto e momentum acelera.
3. Fluxo inicial: CVD com divergencia bullish, mas venda agressiva aparece.
4. Leitura fina: muita agressao de venda de lado, sugerindo comprador passivo absorvendo.
5. Falha compradora: compras relevantes nao empurram preco para cima.
6. Supply nasce: buy aggression falha, depois sell aggression relevante cria zona de oferta.
7. Gatilho: reteste da principal agressao de venda/supply, CVD vira bearish, venda agressiva empurra.
8. Stop: acima do high/supply relevante.
9. Alvo: liquidez abaixo, round number 64k, ordens grandes no book.
10. Gestao: parcial em 2R, fechamento perto de 3R quando chega em zona GEX/ordens.
11. Reversao local: em 64k, sell aggression deixa de empurrar, demanda nasce, buy aggression move preco para cima.

Regra extraida:

```text
Em GEX negativo, operar breakout so quando localizacao + supply/demand + CVD + agressao convergem.
Nao basta romper low. Precisa fluxo real e falha do lado oposto.
```

### Material 2: "maioria dos traders comeca errado"

Tese principal:

- Trading precisa sair de narrativa para processo testavel.
- Setup sem regra codificavel nao e estrategia.
- Edge precisa sobreviver a backtest, Monte Carlo, journal e rejeicao de ideias ruins.
- TPO, orderflow e GEX sao escolhidos porque viram perguntas objetivas.

Regra extraida:

```text
Todo sinal precisa virar boolean/score.
Toda entrada precisa guardar snapshot de features.
Toda hipotese precisa morrer se dados negarem.
```

Impacto no bot:

- Nada de parametro manual no clique.
- Nada de "zona bonita" desenhada no olho.
- Todo supply/demand nasce de eventos mensuraveis.
- Toda versao do setup precisa `run_id`, metricas, amostra e replay.

### Material 3: VWAP

Tese principal:

- VWAP nao e linha magica.
- VWAP mede preco medio aceito por volume, proxy de consenso institucional.
- Preco acima/abaixo da VWAP so importa com contexto de leilao e fluxo.
- Bandas de VWAP funcionam como value area dinamica.
- Esticou 2 sigma com delta perdendo forca: reversao provavel.
- Sustentou VWAP com volume direcional: acceptance provavel.

Regra extraida:

```text
VWAP e filtro de contexto, alvo e zona de reteste.
Nunca gatilho isolado.
```

Uso no bot:

- GEX positivo + balance: fade em bandas, alvo VWAP/POC.
- GEX negativo + imbalance: VWAP serve como reteste de tendencia.
- Divergencia entre preco e VWAP sem orderflow nao gera trade.

### Material 4: Market Profile/AMT

Tese principal:

- Pergunta errada: "vai subir ou cair?"
- Pergunta certa: "onde mercado aceita, rejeita, desequilibra?"
- TPO mostra tempo por preco.
- Volume Profile mostra capital por preco.
- POC = equilibrio.
- VAH/VAL = bordas de valor.
- nPOC = liquidez estrutural nao revisitada.
- LVN = zona oca; pode virar acelerador se romper com fluxo.
- HVN = zona aceita; pode virar magneto/freio.

Regra extraida:

```text
AMT define onde operar e onde nao operar.
Orderflow so autoriza entrada dentro dessas zonas.
```

Uso no bot:

- Preco dentro da VA: preferir reversao ate POC/VWAP.
- Preco aceitando fora da VA: preferir continuacao.
- Rompimento por LVN + GEX negativo + CVD alinhado: setup de momentum.
- Sweep fora da VA + retorno rapido + absorcao oposta: failed auction.

### Tweets e teses complementares

GEX:

- Gamma positivo: market maker amortece, compra fundo, vende topo.
- Gamma negativo: market maker amplifica, compra alta, vende baixa.
- Gamma flip separa ambiente de compressao e expansao.
- Niveis de maior OI/gamma viram paredes, imas ou pontos de inventario.

Breakouts:

- Maioria falha.
- Filtro necessario: AMT diz se mercado esta pronto para sair do range; orderflow mostra se ha fluxo real.
- GEX negativo aumenta chance de continuidade depois de rompimento.

Orderflow:

- Agressao relevante precisa mover preco.
- Se agressao nao move, ha absorcao.
- Se fluxo seca no extremo, ha exaustao.
- CVD confirma ou diverge.

Supply/demand:

- Zona valida nao nasce de candle.
- Zona valida nasce de agressao falha + resposta oposta + defesa/reteste.

### Conflitos e limites detectados

1. GEX gratuito e proxy.
   OI + gamma nao mostram posicao liquida real do dealer. Usar como regime probabilistico, nao verdade.

2. Bookmap gratuito exige captura propria.
   Snapshot atual nao basta para replay. Precisa salvar depth diffs continuamente.

3. Footprint historico completo nao vem pronto.
   Klines nao bastam. Precisa trades tick/aggTrade e reconstrucao por bucket.

4. "HFT" via Binance retail nao e HFT institucional.
   Sistema pode ser low-latency, mas nao competir por microssegundos.

5. Setup discricionario original tem nuances visuais.
   Bot precisa reduzir tudo a features: delta_z, price_efficiency, OBI, replenish, acceptance, rejection.

### Regras canonicas do passo 2

```text
1. Definir regime:
   GEX positivo, negativo, neutro, stale.

2. Definir localizacao:
   VAH, VAL, POC, VWAP band, LVN, HVN, nPOC, high/low, round number, GEX strike.

3. Definir estado AMT:
   balance, rejection, acceptance, imbalance, failed auction.

4. Definir zona:
   supply/demand so nasce de agressao falha + agressao oposta.

5. Definir gatilho:
   CVD + agressao relevante + absorcao/exaustao + book behavior.

6. Definir execucao:
   entrada, stop, alvo, TTL, slippage maximo, cancelamento.

7. Definir invalidez:
   acceptance contra posicao, dado stale, spread alto, orderbook desync, fluxo oposto.

8. Definir prova:
   replay, backtest, paper, metricas em R, custos e slippage.
```

Resultado revisado:

```text
Bot nao adivinha direcao por narrativa.
Bot estima expectancy condicional por cenario real:
regime + leilao + fluxo + liquidez + execucao -> distribuicao de retorno em R.
```

Isto nao e media simples. Pode comecar como regras, mas objetivo final e tabela/modelo de probabilidades condicionado ao contexto:

```text
P(win), avg_win_R, avg_loss_R, expectancy_R, max_adverse_excursion, max_favorable_excursion
por setup + regime + zona + fluxo + horario + volatilidade + liquidez.
```

Dados ocultos existem e precisam ser tratados como limite estrutural:

- posicao liquida real de dealers/market makers;
- inventario interno e hedge exato;
- ordens iceberg/hidden;
- fila real em cada nivel do book;
- stops reais de usuarios;
- fluxo OTC;
- latencia/roteamento interno da exchange;
- liquidacoes antes de aparecerem publicamente.

Logo sistema nao deve afirmar "market maker vai fazer X". Deve medir:

```text
Quando proxy GEX negativo + LVN break + sell aggression + bid pull ocorreu,
qual foi distribuicao real dos proximos 5s/15s/60s?
```

## 2. Limite realista de "HFT"

Nao chamar isto de HFT puro. Binance retail/API publica entrega dados em janelas como 100ms/250ms, internet comum adiciona latencia, e nao ha colocacao direta no matching engine.

Meta correta:

- Motor low-latency event-driven.
- Decisao em milissegundos locais.
- Execucao por WebSocket/REST com controle de stale data.
- Sem polling no hot path.
- Sem Python no hot path final, exceto pesquisa/backtest. Hot path preferido: Rust + Tokio, Go, ou C++.

## 3. Fontes gratuitas/substitutos de ferramentas pagas

| Ferramenta paga | Versao gratuita propria | Fonte/dado |
|---|---|---|
| Bookmap | Heatmap L2 proprio | Binance `depth@100ms`, snapshot REST |
| Footprint | Footprint por bucket de trades | `aggTrade`/`trade`, lado agressor via `m` |
| TPO/Market Profile | TPO engine propria | trades/klines capturados |
| Volume Profile | VP engine propria | trades agregados por bin de preco |
| VWAP | VWAP session/anchored propria | trades com preco * volume |
| GEX/GammaFlip | Proxy GEX propria | Binance Options `exchangeInfo`, `openInterest`, `mark` |
| Journal | Event store + metrics | parquet/sqlite/postgres |

### 3.1 Matriz real de dados

| Dado necessario | Fonte gratuita primaria | Como usar | Limite |
|---|---|---|---|
| Preco negociado tick/agregado | Binance Futures `aggTrade` ou `trade` | CVD, delta, footprint, VWAP, volume profile | `aggTrade` agrega prints; `trade` e mais granular se disponivel |
| Lado agressor | Campo `m` do trade Binance | `m=true` => buyer maker => agressor vendedor | Inferencia por regra da exchange, nao intencao real |
| Book L2 diff | Binance `depth@100ms` | heatmap, walls, pull/stack, OBI, microprice | RPI/hidden/iceberg nao visiveis |
| Book snapshot | REST `/fapi/v1/depth?limit=1000` | inicializar/resync orderbook local | snapshot pontual, nao historico |
| Best bid/ask | `bookTicker` | spread, microprice, exec quality | topo do book apenas |
| Mark/funding | `/fapi/v1/premiumIndex`, mark stream | filtro de funding, mark/last divergence | nao mostra posicao |
| Liquidacoes | `forceOrder` stream | squeeze/stop cascade proxy | amostrado/limitado pela exchange |
| OI perp/futures | `/fapi/v1/openInterest`, hist OI | contexto de alavancagem | nao separa long/short real |
| Options chain | Binance Options `exchangeInfo` | expiries/strikes/call/put | cobertura Binance pode ser menor que Deribit |
| Options OI | Binance Options `/eapi/v1/openInterest` | GEX proxy por strike | OI nao revela dealer net |
| Options Greeks | Binance Options `/eapi/v1/mark` | gamma/delta/IV por opcao | mark model da exchange |
| Cross-exchange options | Deribit public API, OKX public API | GEX melhor para BTC global | precisa normalizar contratos |
| MMT/Bookmap concepts | documentacao/conceitos publicos | referencia de features: footprint, TPO, heatmap, CVD | nao entra no runtime |
| Historico tick/depth | Captura propria | backtest/replay real | precisa rodar coletor 24/7 |
| Historico pago opcional | Tardis/Amberdata/Kaiko etc | acelerar backtest profundo | pago, nao versao gratuita |

Regra de projeto:

```text
Se dado nao vem de API/public feed/captura propria, nao entra no motor automatico.
Pode entrar em research manual, nao em producao.
```

### 3.2 Fontes adicionais por exchange

| Exchange/fonte | Dados uteis | Uso no sistema | Observacao |
|---|---|---|---|
| Binance USD-M | trades, depth, bookTicker, mark/funding, OI futures, liquidacoes | execucao/paper principal + fluxo | melhor para rodar bot BTCUSDT |
| MEXC Futures | trades, depth, fair/index price, funding, account/trade API | candidato para execucao 30s scalp | fees/API/regiao/limites precisam ser validados antes |
| Binance Options | option chain, OI, mark greeks | GEX proxy local | universo pode ser menor que Deribit |
| Deribit | options OI/greeks/ticker, book, trades | GEX BTC global e validacao de strikes | fonte publica forte para options |
| OKX | spot/perp/options, OI, books, trades, greeks conforme endpoint | segunda fonte GEX/flow | bom cross-check |
| Bybit | books, trades, funding, OI, liquidacoes, historico publico | fluxo perp/cross-exchange | bom para confirmar liquidez/lead-lag |
| Hyperliquid | l2Book, trades, BBO, OI/funding | perp flow alternativo | dados on-chain/perp relevantes |
| MMT free/pro | footprint, CVD, VWAP, heatmap, TPO | referencia conceitual opcional | nao assumir API automatica sem contrato |
| GammaFlip | GEX tratado | benchmark/pago | substituir proxy se contratar |
| Hyblock | liquidity maps/heatmaps/liquidations | benchmark/pago | comparar com nosso heatmap |
| Laevitas/Greeks.live | options, IV, gamma/OI | benchmark/pago/free parcial | validar GEX |
| Velo/Coinglass/Coinalyze | OI/funding/liquidations/ratios | contexto macro de derivativos | pode ter API paga |
| Tardis/Amberdata/Kaiko | historico tick/depth/options normalizado | backtest profundo | pago, acelera pesquisa |

Arquitetura aceita adaptadores:

```text
data_adapter_binance
data_adapter_mexc
data_adapter_deribit
data_adapter_okx
data_adapter_bybit
data_adapter_hyperliquid
data_adapter_mmt_optional
data_adapter_paid_optional
```

Cada adaptador precisa entregar evento normalizado:

```text
Trade { exchange, symbol, ts_exchange, ts_local, price, qty, aggressor_side }
DepthDelta { exchange, symbol, ts_exchange, ts_local, bids, asks, sequence }
Ticker { exchange, symbol, bid, ask, mark, index, funding }
OpenInterest { exchange, instrument, ts, oi, notional }
OptionGreek { exchange, option_symbol, expiry, strike, cp, mark, iv, delta, gamma, vega, theta, oi }
Liquidation { exchange, symbol, ts, side, price, qty }
```

### 3.3 Dados ocultos e proxies

Dados ocultos de verdade:

- dealer net position real;
- inventario market maker;
- hedge exato em spot/perp/futures/options;
- stops reais de usuarios;
- iceberg/hidden orders;
- fila real na frente da nossa ordem;
- fluxo OTC;
- roteamento/latencia interna da exchange;
- liquidacoes antes de aparecerem publicamente;
- posicao agregada de whales/fundos fora de exchange.

Nao ha API publica limpa para isto. Plano correto:

```text
dado oculto -> proxy observavel -> validar se proxy melhora expectancy fora da amostra
```

| Dado oculto | Proxy observavel | Como medir |
|---|---|---|
| Hidden liquidity/iceberg | execucao grande no nivel sem consumir displayed size | traded_qty_at_level > displayed_qty + replenish |
| Absorcao passiva | agressao forte sem deslocamento | delta_z alto contra price_change baixo |
| Spoof/pull | wall some antes do toque sem trade | vanish_before_touch_rate |
| Stops acima/abaixo | equal highs/lows, prior high/low, round number, liquidation clusters | distance_to_liquidity_pool |
| Dealer hedge | GEX proxy + spot/perp reaction perto de strike | response study por strike |
| Pressao de liquidacao | forceOrder + OI drop + candle expansion | liquidation_impulse_score |
| Posicao crowded | funding extremo + OI alto + long/short ratios pagos/opcionais | crowded_side_score |
| Queue position | volume na frente estimado no momento da ordem | displayed_qty_ahead - trade_through - cancels |
| OTC/invisivel | basis muda sem trade local proporcional, lead-lag cross-exchange | cross_exchange_dislocation |

Regra:

```text
Proxy sem ganho estatistico = ruido.
Proxy com ganho fora da amostra = feature.
```

### 3.4 Bookmap/MMT sem dependencia visual

Bookmap/MMT aparecem nos materiais como ferramentas de leitura de orderflow/liquidez, mas o projeto nao precisa de nada visual para operar.

O que importa e o dado subjacente:

- depth/orderbook updates;
- trades assinados por agressor;
- snapshots de book;
- TPO/volume profile calculado;
- CVD/delta;
- VWAP;
- options OI/greeks para GEX proxy;
- liquidations/OI/funding quando disponivel.

Regra:

```text
Sem dashboard visual obrigatorio.
Sem MMT/Bookmap como dependencia.
Sem screen scraping.
Somente API/feed/captura propria/replay.
```

Equivalencia data-driven:

| Conceito visual | Feature numerica |
|---|---|
| Heatmap | wall_quality, liquidity_persistence, pull_stack_score |
| Volume bubbles | aggressive_trade_size_z, delta_burst_z |
| Footprint | bid_volume, ask_volume, delta, imbalance por price bin |
| CVD | CVD slope/divergence por janela |
| TPO | VAH/VAL/POC, acceptance/rejection |
| Iceberg/stops tracker | hidden_liquidity_score, stop_pool_distance |
| Replay visual | deterministic event replay + feature hashes |

MMT/Bookmap podem ser citados como inspiracao de feature set, mas nao entram em arquitetura, roadmap critico ou runtime.

## 4. Dados Binance

### 4.1 Execucao/paper recomendado

Dois modos, ambos necessarios:

- `paper_live`: usa market data live real, simula fills internamente. Melhor para validar edge.
- `futures_demo`: usa Binance Futures Demo/Testnet para testar envio/cancelamento/estado de ordens. Melhor para plumbing de execucao.

Para short/long de BTCUSDT, usar USD-M Futures Demo. Spot Testnet nao serve bem para short direcional.

Endpoints oficiais USD-M Futures testnet:

- REST testnet: `https://demo-fapi.binance.com`
- WebSocket market testnet: `wss://demo-fstream.binance.com`
- WebSocket API testnet: `wss://testnet.binancefuture.com/ws-fapi/v1`

### 4.2 Market data hot path

Streams principais:

- `btcusdt@aggTrade`: trades agregados, 100ms em USD-M Futures.
- `btcusdt@depth@100ms`: diff book depth, 100ms se disponivel.
- `btcusdt@bookTicker`: melhor bid/ask, spread e microprice.
- `btcusdt@markPrice`: mark price/funding, filtro de risco.
- `btcusdt@forceOrder`: liquidacoes, opcional.

Order book local:

1. Abrir stream de depth.
2. Buffer de eventos.
3. Buscar snapshot REST `/fapi/v1/depth?symbol=BTCUSDT&limit=1000`.
4. Ignorar evento antigo.
5. Primeiro evento processado precisa cobrir `lastUpdateId`.
6. Cada novo evento precisa ter `pu == u_anterior`; se falhar, resetar snapshot.
7. Quantidade 0 remove nivel.

### 4.3 GEX gratuito por Binance Options

Dados:

- `GET /eapi/v1/exchangeInfo`: lista expiries, strikes, call/put, unit.
- `GET /eapi/v1/openInterest`: OI por underlying/expiration.
- `GET /eapi/v1/mark`: mark price e gregas, incluindo gamma.

Limite: OI publico nao revela posicao liquida do dealer. Logo GEX proprio e proxy, nao verdade absoluta. Se depois integrarmos provedor tipo gammaflip, ele substitui este modulo sem mudar estrategia.

Alternativas gratuitas/semifracas para GEX:

| Fonte | O que entrega | Uso |
|---|---|---|
| Binance Options | OI + mark greeks por opcao | proxy GEX local Binance |
| Deribit public API | options summaries, OI, greeks/ticker | melhor universo BTC options em muitos cenarios |
| OKX public API | options instruments, tickers, greeks/OI conforme endpoint | segunda fonte cross-check |
| Greeks.live/free pages | leitura visual/manual | pesquisa, nao producao sem API |
| GammaFlip | GEX tratado/visual | pago/parceiro; benchmark, nao dependencia inicial |

Formula proxy:

```text
raw_gex = gamma * open_interest * contract_size * spot^2 * 0.01
```

Problema:

```text
raw_gex != dealer_gex_real
```

Porque falta:

- quem esta long/short opcao;
- quanto dealer carregou;
- hedge em spot/perp/futures;
- trades OTC;
- netting entre exchanges;
- mudanca intraday de posicao.

Uso correto:

```text
GEX proxy = feature de regime.
Nao usar como oraculo.
Validar empiricamente se proxy melhora expectancy.
```

## 5. Modelos de estado

### 5.1 Perfil de leilao

Sessao:

- Default: diario UTC.
- Perfis extras: Asia, Londres, NY, semanal, 60 dias composite.

Binning:

```text
tick_bin = max(exchange_tick_size * 10, round(ATR_1m * 0.05))
```

TPO:

- `TPO_count[price_bin] += 1` por intervalo fixo onde preco negociou.
- `POC_TPO`: bin com maior tempo.
- `VA`: 70% dos TPOs ao redor do POC.
- `VAH/VAL`: extremos da value area.

Volume Profile:

- `volume[price_bin] += traded_qty`.
- `VPOC`: maior volume.
- `HVN`: volume z-score alto.
- `LVN`: volume z-score baixo entre dois HVNs.
- `nPOC`: POC de sessao anterior ainda nao tocado.

Classificacao AMT:

| Estado | Condicao mecanica |
|---|---|
| Balance | preco dentro de VA, VWAP plana, ranges sobrepostos, GEX positivo/neutro |
| Rejection | sweep fora de VA e retorno rapido para dentro |
| Acceptance | N prints/closes fora da VA + volume real + reteste segura |
| Imbalance | deslocamento por LVN, CVD acompanha, VWAP inclina |
| Failed auction | rompe extremo, nao aceita, fluxo oposto absorve/agride |

Parametros iniciais:

```yaml
acceptance_prints: 3
acceptance_seconds: 15
rejection_return_seconds: 20
lvn_break_min_ticks: 3
profile_value_area_pct: 0.70
```

### 5.2 VWAP

VWAP session:

```text
vwap = sum(price * qty) / sum(qty)
```

Bandas:

- `vwap +/- 1 sigma`
- `vwap +/- 2 sigma`
- sigma ponderado por volume.

Uso:

- Gamma positivo + balance: VWAP/POC atraem preco; operar reversao nas bandas/extremos.
- Gamma negativo + imbalance: VWAP vira zona de reteste, nao alvo primario.

### 5.3 GEX

Proxy por strike:

```text
gex_1pct_usd =
  gamma * open_interest_contracts * contract_unit * spot_price^2 * 0.01 * dealer_sign
```

`dealer_sign` nao pode ser inferido com certeza via OI publico. Config default:

```yaml
dealer_sign_model: configurable
default_call_sign: -1
default_put_sign: -1
```

Leituras:

- `total_gex > +threshold`: regime amortecedor.
- `total_gex < -threshold`: regime expansivo.
- `gamma_flip`: nivel onde GEX acumulado troca sinal.
- `max_gex_wall`: strike com maior GEX positivo absoluto.
- `max_neg_gex`: strike com maior GEX negativo absoluto.
- `gamma_vacuum`: faixa entre strikes com baixa concentracao de GEX.

Regime:

| Regime | Mecanica esperada | Setup preferido |
|---|---|---|
| GEX+ | dealer compra queda/vende alta, rompe e devolve | mean reversion em VAH/VAL/VWAP bands |
| GEX- | dealer vende queda/compra alta, movimento acelera | breakout por LVN/low/high |
| GEX neutral | sem vantagem clara | reduzir tamanho ou nao operar |

Refresh:

- Options chain: 30s a 120s.
- Recalcular se spot cruza strike relevante.
- Invalidar GEX se snapshot > 180s.

### 5.4 Orderflow

Assinatura de trades:

Em Binance, `m=true` significa comprador e maker; agressor foi vendedor.

```text
signed_qty = if m == true then -qty else +qty
signed_notional = signed_qty * price
CVD += signed_notional
```

Janelas:

- `250ms`: micro impulso.
- `1s`: gatilho.
- `5s`: confirmacao.
- `15s`: contexto do micro range.
- `60s`: tendencia intraday curta.

Metrica:

```text
delta_z = (delta_window - mean_delta_rolling) / std_delta_rolling
price_efficiency = abs(price_change_ticks) / max(1, abs(delta_notional))
```

Agressao relevante:

```text
abs(delta_z_1s) >= 2.0
or abs(delta_z_5s) >= 2.5
```

Absorcao:

```text
sell_absorption =
  delta_z_5s <= -2.0
  and price_change_ticks >= -1
  and bid_depth_replenish_z >= 1.5
  and trades_clustered_near_level == true

buy_absorption =
  delta_z_5s >= 2.0
  and price_change_ticks <= 1
  and ask_depth_replenish_z >= 1.5
  and trades_clustered_near_level == true
```

Exaustao:

```text
exhaustion_down =
  lower_low == true
  and CVD_lower_low == true
  and trade_volume_z falling
  and no_depth_follow_through == true

exhaustion_up = espelho
```

Divergencia CVD:

```text
bullish_div = price_lower_low and CVD_higher_low
bearish_div = price_higher_high and CVD_lower_high
```

### 5.5 Heatmap/Bookmap proprio

Heatmap nao e so "parede grande no book". E historico tempo-preco de liquidez passiva + trades agressivos batendo nessa liquidez.

Dados brutos:

- depth diff L2 em tempo real;
- snapshot L2 para resync;
- trades assinados por agressor;
- best bid/ask;
- timestamps locais e exchange;
- opcional: depth multi-exchange normalizado.

Estado por nivel de preco:

```text
level_state[price] = {
  bid_qty,
  ask_qty,
  first_seen_ts,
  last_update_ts,
  cumulative_added_qty,
  cumulative_removed_qty,
  traded_through_qty,
  touches,
  survived_touches,
  vanished_before_touch,
  replenished_after_hit
}
```

Orderbook features:

```text
obi_N = (sum_bid_qty_N - sum_ask_qty_N) / (sum_bid_qty_N + sum_ask_qty_N)
microprice = (best_ask * bid_qty + best_bid * ask_qty) / (bid_qty + ask_qty)
depth_wall_z = (level_qty - mean_level_qty) / std_level_qty
```

Resting liquidity valida:

- size z-score >= 3.
- persistencia >= 2s.
- nao some antes do toque em mais de 70% dos casos recentes.

Pull/stack:

- `stack_bid`: bid aumenta perto do preco.
- `pull_bid`: bid some antes do toque.
- `stack_ask`: ask aumenta perto do preco.
- `pull_ask`: ask some antes do toque.

Score de parede real:

```text
wall_quality =
  size_z
  + persistence_z
  + touch_survival_rate
  + replenish_after_hit_z
  - vanish_before_touch_rate
  - spoof_cancel_z
```

Eventos Bookmap-like:

| Evento | Condicao |
|---|---|
| Liquidity wall | `wall_quality >= threshold` |
| Spoof/pull | parede grande some antes do toque, sem trade correspondente |
| Absorption | agressao bate nivel, preco nao anda, qty reabastece |
| Exhaustion | trades agressivos diminuem perto do extremo, book oposto segura |
| Vacuum | baixa liquidez entre dois HVN/walls, preco atravessa rapido |
| Liquidity magnet | wall/nPOC/round number atrai preco repetidamente |

Renderizacao opcional:

```text
x = tempo
y = preco
cor = log(qty resting)
bolhas = market trades assinados
linhas = VWAP/POC/VAH/VAL/GEX strikes
```

Para automacao, heatmap vira features numericas. Imagem e so debug.

## 6. Zonas de oferta/demanda

Supply nasce quando:

```text
buy_aggression_relevant == true
and price_fails_to_advance == true
and sell_aggression_relevant_after == true
and local_high_defined == true
```

Zona:

```text
supply.low = first_failed_buy_cluster_price
supply.high = local_swing_high + buffer_ticks
supply.anchor = largest_sell_aggression_price
```

Demand nasce quando:

```text
sell_aggression_relevant == true
and price_fails_to_break_down == true
and buy_aggression_relevant_after == true
and local_low_defined == true
```

Zona:

```text
demand.low = local_swing_low - buffer_ticks
demand.high = first_failed_sell_cluster_price
demand.anchor = largest_buy_aggression_price
```

Zona expira:

- tocada 3 vezes.
- rompida com acceptance.
- idade > 30min em scalping.
- contexto GEX/AMT muda.

## 6.1 Price action e SMC

Nos materiais enviados, SMC aparece mais como comparacao/critica:

```text
SMC "diz onde tem liquidez", com aspas.
GEX tenta explicar por que dealer pode mover/amortecer movimento.
```

Logo SMC pode entrar, mas como modulo auxiliar de localizacao, nao como narrativa central.

Componentes SMC/price action automatizaveis:

| Conceito | Regra mecanica |
|---|---|
| Swing high/low | pivots por fractal/zigzag com threshold de ATR/ticks |
| Liquidity pool | clusters de equal highs/lows, prior highs/lows, round numbers |
| Sweep | rompe swing/level por X ticks e retorna em Y segundos |
| BOS | rompe swing com acceptance + volume/flow |
| CHOCH | quebra estrutura oposta depois de sweep/absorpcao |
| Displacement | range expansion + delta alinhado + atravessa LVN |
| Fair value gap/imbalance | candle/intervalo com baixa negociacao entre bins + deslocamento |
| Order block | ultima zona oposta antes de displacement, validada por orderflow |

Regras de uso:

- SMC marca possivel liquidez.
- AMT diz se local e valor/rejeicao/desequilibrio.
- GEX diz se ambiente tende a amortecer ou amplificar.
- Orderflow autoriza entrada.

Nao usar:

```text
"smart money manipulou" como feature.
```

Usar:

```text
prior_low_swept == true
sell_aggression_absorbed == true
price_returned_inside_value == true
```

## 7. Setups mecanicos

### 7.1 Short breakout em GEX negativo

Inspirado no scalp transcrito.

Precondicoes:

```text
regime == GEX_NEGATIVE
price near LVN/volume_low or prior_low or round_number
liquidity_below exists
AMT state in acceptance_down or imbalance_down candidate
supply_zone_recent == true
```

Gatilho:

```text
price retests supply_zone.anchor or lower edge
buy_aggression_fails == true
sell_aggression_relevant == true
CVD_1s/5s bearish
best_bid gets hit or pulled
spread <= max_spread
```

Entrada:

- Limit agressiva no bid/ask conforme fila e spread, ou market se breakout ja iniciou e slippage esperado < limite.
- Stop acima de `supply.high + buffer`.
- Invalida se preco aceita acima do supply.

Alvos:

1. `1R`: reduzir risco ou mover stop para BE se fill/slippage permitir.
2. `2R`: parcial 50%.
3. Liquidez abaixo: prior low, round number, depth wall, nPOC, max negative GEX strike.
4. Sair se sell aggression para de mover preco e demand/absorption aparece.

### 7.2 Long breakout em GEX negativo

Espelho:

```text
regime == GEX_NEGATIVE
price near LVN/prior_high
demand_zone_recent == true
sell_aggression_fails
buy_aggression_relevant
CVD bullish
asks pulled or lifted
```

Stop abaixo de demand. Alvos em highs, round numbers, nPOC acima, GEX levels.

### 7.3 Mean reversion em GEX positivo

Precondicoes:

```text
regime == GEX_POSITIVE
AMT state == Balance
price at VAH/VAL or VWAP +/- 2 sigma or HVN edge
no acceptance outside value
```

Short no topo:

```text
price above/at VAH or upper_band
buy_aggression_exhaustion or buy_absorption
bearish_CVD_div or ask_stack
```

Long no fundo:

```text
price below/at VAL or lower_band
sell_aggression_exhaustion or sell_absorption
bullish_CVD_div or bid_stack
```

Alvos:

- VWAP.
- POC/VPOC.
- lado oposto da VA se fluxo continuar.

Stop:

- fora do extremo rejeitado.
- cancelar trade se acceptance fora da VA.

### 7.4 Failed auction

Precondicoes:

```text
price sweeps prior high/low or VA edge
liquidity taken
no continuation
opposite absorption/aggression appears
```

Direcao:

- Sweep de high + buy fails + sell aggression -> short.
- Sweep de low + sell fails + buy aggression -> long.

Alvo:

- POC/VWAP primeiro.
- VAL/VAH oposto se range forte.

### 7.5 No-trade

Esta lista nao veio pronta dos textos. E camada de protecao operacional + hipoteses a testar.

Separar em 2 tipos:

#### Hard no-trade

Bloqueio por integridade. Nao depende de backtest, porque dado/execucao esta quebrado.

- GEX stale se setup usa GEX.
- orderbook desincronizado.
- trade stream atrasado/stale.
- spread acima do limite executavel.
- latencia acima do limite executavel.
- exchange/user stream sem confirmacao de estado.
- tamanho minimo da exchange obriga risco maior que permitido.
- kill switch diario/semanal ativo.

#### Soft no-trade / penalty

Hipoteses. Precisam ser testadas por replay/ablation.

- funding/evento macro perto.
- OPEX/expiry causando regime instavel.
- AMT balance + GEX negativo + fluxo fraco.
- AMT trend + GEX positivo + breakout sem acceptance.
- ultima perda teve slippage > 0.5R.
- horario com historico ruim.
- volatilidade abaixo/acima da faixa ideal.

Regra:

```text
Hard gate bloqueia.
Soft gate vira feature/penalty e so bloqueia se dados provarem expectancy ruim.
```

## 8. Scoring de decisao

Versao anterior `score >= 75` era placeholder. Nao veio do material. Deve ser substituido por pesquisa empirica.

Objetivo:

```text
Aprender quais combinacoes de features geram expectancy positiva em dados reais.
```

Features candidatas:

```text
gex_regime
distance_to_gamma_flip
distance_to_max_gex_wall
AMT_state
distance_to_VAH_VAL_POC
distance_to_VWAP_bands
LVN/HVN context
nPOC distance
supply/demand freshness
CVD_slope_1s_5s_15s
delta_z_1s_5s
absorption_score
exhaustion_score
OBI
wall_quality
pull_stack_score
spread_ticks
latency_ms
time_of_day
volatility_regime
funding_distance_time
```

Labels:

```text
future_return_R_5s
future_return_R_15s
future_return_R_60s
MAE_R
MFE_R
hit_1R_before_-1R
hit_2R_before_-1R
slippage_R
```

Modelos em ordem:

1. Regras booleanas transparentes.
2. Grid/Optuna para thresholds.
3. Logistic regression para `P(hit_1R_before_stop)`.
4. Gradient boosted trees para nao-linearidade.
5. Contextual bandit so depois de paper robusto.

Validacao obrigatoria:

```text
walk-forward
purged time-series split
sem lookahead
custos/slippage incluidos
ablation por feature group
Monte Carlo de ordem dos trades
```

Exemplo de pesquisa para short breakout:

```text
Filtro base:
  setup == short_breakout_candidate

Comparar grupos:
  A: AMT only
  B: AMT + orderflow
  C: AMT + orderflow + GEX proxy
  D: AMT + orderflow + GEX proxy + heatmap

Aceitar GEX no setup so se C > B fora da amostra.
Aceitar heatmap so se D > C fora da amostra.
```

Sizing:

- Enquanto nao ha 500+ trades fora da amostra: tamanho minimo/paper.
- Depois: tamanho proporcional a expectancy e drawdown, capado por risco maximo.
- Nunca aumentar posicao porque score "parece lindo"; aumentar so por evidencia estatistica.

## 9. Risco e capital R$100

Capital logico:

```yaml
starting_capital_brl: 100
base_currency: BRL
execution_quote: USDT
brl_usdt_rate_source: config_or_live_USDTBRL
```

Risco:

```yaml
risk_per_trade_brl: 0.50
max_daily_loss_brl: 2.00
max_weekly_loss_brl: 5.00
max_trades_per_day: 8
max_concurrent_positions: 1
max_leverage_demo: 1
```

Sizing:

```text
risk_usdt = risk_brl / usdtbrl
stop_distance_usdt = abs(entry - stop)
qty_btc = risk_usdt / stop_distance_usdt
notional_usdt = qty_btc * entry
```

Caps:

- `notional_usdt <= capital_usdt * max_leverage_demo`
- respeitar minQty/stepSize/tickSize.
- se minQty exigir risco maior que permitido, nao operar ou usar paper interno sem exchange order.

Stops:

- Sempre stop logico no risk engine.
- Em demo/live futuro, enviar stop reduce-only quando possivel.
- Kill switch fecha/cancela se feed some, orderbook perde sync, ou posicao diverge do ledger.

### 9.1 Risco para scalp de 30s

Horizonte:

```yaml
target_holding_seconds: 30
max_holding_seconds: 45
decision_windows_ms: [250, 1000, 5000, 15000, 30000]
```

Mudanca principal:

- setup precisa edge antes de 30s;
- fee/slippage dominam resultado;
- stop precisa ser tecnico e executavel, nao "longe o bastante";
- market order so se slippage esperado couber no R;
- se target bruto nao paga fee + slippage + buffer, trade bloqueado.

Labels especificos:

```text
return_R_5s
return_R_15s
return_R_30s
hit_target_before_stop_30s
MAE_R_30s
MFE_R_30s
time_to_target_ms
time_to_stop_ms
spread_paid_R
fee_paid_R
slippage_R
```

Hard gates 30s:

- spread maior que 10% do stop;
- expected round-trip fee maior que 35% do alvo;
- expected slippage maior que 20% do stop;
- orderbook lag > 300ms;
- trade stream stale > 1000ms;
- target liquidity antes de 1R inexistente;
- ATR/volatilidade 30s menor que fee+slippage;
- fila estimada em maker nao deve preencher antes do sinal expirar.

### 9.2 Alavancagem dinamica

Alavancagem nao e parametro fixo. E saida do risk engine.

Formula:

```text
gross_loss_pct =
  stop_distance_pct
  + entry_fee_pct
  + exit_fee_pct
  + expected_slippage_pct
  + liquidation_buffer_pct

max_notional_by_risk = risk_usdt / gross_loss_pct
max_leverage_by_risk = max_notional_by_risk / equity_usdt
leverage_used = min(exchange_max_leverage, max_leverage_by_risk, config_hard_cap)
```

Se usar apenas fracao da conta como margem:

```text
position_margin_usdt = equity_usdt * margin_fraction
notional_usdt = position_margin_usdt * leverage_used
loss_usdt = notional_usdt * gross_loss_pct
loss_usdt <= risk_usdt
```

Regra:

```text
Usuario pode configurar exchange_max_leverage = 500.
Bot nao usa 500x se stop+fee+slippage+buffer tornam risco maior que limite.
```

### 9.3 500x: tratamento obrigatorio

500x implica margem inicial aproximada de:

```text
1 / 500 = 0.20% do notional
```

Em 500x, movimento adverso muito pequeno pode liquidar antes do stop funcionar. Fee tambem vira enorme contra margem.

Exemplo MEXC API anunciado para 2026-06-01:

```text
maker: 0.06% por lado
taker: 0.08% por lado
taker round-trip: 0.16% notional
```

Comparacao:

```text
margem 500x ~= 0.20% notional
fee taker round-trip ~= 0.16% notional
```

Conclusao tecnica:

```text
Em 500x, round-trip taker pode consumir ~80% da margem inicial antes de PnL.
Logo 500x nao e modo live inicial; e modo paper/stress test.
```

Politica:

```yaml
leverage_exchange_max_configurable: 500
leverage_live_hard_cap_initial: 20
leverage_paper_stress_cap: 500
min_liquidation_buffer_pct_of_stop: 3.0
max_fee_to_target_ratio: 0.35
max_slippage_to_stop_ratio: 0.20
max_margin_fraction_per_trade: 0.02
```

Gate para liberar acima de 20x:

- paper 30s com 1000+ trades;
- expectancy positiva apos fees MEXC reais;
- max drawdown dentro do limite;
- nenhum stop perdido por liquidation-before-stop em simulacao;
- latency p95 abaixo do limite;
- fill model validado contra fills reais demo/live micro.

### 9.4 Liquidation-before-stop guard

Antes de qualquer ordem:

```text
estimated_liq_price = exchange_formula_or_conservative_approx(position, leverage, maintenance_margin)
liq_distance_pct = abs(entry - estimated_liq_price) / entry
stop_distance_pct = abs(entry - stop) / entry

allow_trade =
  liq_distance_pct >= stop_distance_pct * min_liquidation_buffer_pct_of_stop
```

Se formula exata da exchange nao estiver implementada:

```text
usar aproximacao conservadora
ou bloquear alavancagem alta
```

### 9.5 MEXC como venue

MEXC entra como venue candidata, nao substituto automatico da Binance.

Validar antes:

- API futures disponivel para regiao/conta;
- endpoints de order/cancel funcionando;
- fee API atual por par;
- max leverage por contrato;
- risk limits;
- minQty/tickSize/stepSize;
- liquidation formula;
- WebSocket depth/trades confiavel 24/7;
- private order stream confiavel;
- rate limits;
- demo/testnet disponivel ou paper-live interno.

Adapter:

```text
execution_adapter_mexc
market_data_adapter_mexc
risk_model_mexc
fee_model_mexc
liquidation_model_mexc
```

Fallback:

```text
Se MEXC trade API instavel/indisponivel, usar MEXC apenas como data source.
Execucao fica Binance/venue com API comprovada.
```

## 10. Execucao

Estado da ordem:

```text
IDLE -> SIGNAL_ARMED -> ORDER_SENT -> PARTIAL_FILLED -> OPEN
OPEN -> REDUCE_1R/2R -> EXIT_SENT -> FLAT
any -> CANCEL_PENDING -> FLAT
any -> KILL_SWITCH
```

Regras:

- Uma ordem por sinal.
- `clientOrderId` idempotente.
- Nunca duplicar ordem depois de timeout; consultar user stream/order status.
- Cancelar ordem se nao preencher em `entry_ttl_ms`.
- Se fill parcial abaixo de minimo operacional, fechar ou cancelar resto.

Parametros:

```yaml
max_market_order_slippage_ticks: 3
entry_ttl_ms: 750
max_data_age_ms: 300
max_order_roundtrip_ms_warn: 250
max_spread_ticks: 2
post_signal_cooldown_ms: 1500
```

Fill model para `paper_live`:

- Market buy: preencher no melhor ask + slippage model.
- Market sell: melhor bid - slippage model.
- Limit: preencher se preco toca e fila estimada consumida.
- Fees: usar fee configurada maker/taker.
- Slippage cresce com spread, OBI contra, volatilidade de 1s e tamanho relativo ao depth.

## 11. Arquitetura

Stack alvo:

```text
Rust + Tokio
```

Motivo:

- baixa latencia com async IO;
- controle forte de memoria/concorrencia;
- bom para WebSocket 24/7;
- hot path previsivel;
- deploy unico sem runtime pesado.

Crates provaveis:

```toml
tokio = { features = ["full"] }
tokio-tungstenite
reqwest
serde
serde_json
chrono
rust_decimal or ordered-float
crossbeam
arc-swap
tracing
tracing-subscriber
metrics
metrics-exporter-prometheus
anyhow / thiserror
config
arrow / parquet
sqlx or rusqlite
```

```text
market_data_gateway
  -> normalizer
  -> event_bus/ring_buffer
  -> orderbook_engine
  -> trade_aggregator
  -> profile_engine
  -> vwap_engine
  -> gex_engine
  -> feature_engine
  -> signal_engine
  -> risk_engine
  -> execution_engine
  -> journal/replay
```

### 11.1 Tasks Tokio 24/7

```text
supervisor_task
  -> binance_ws_task
  -> deribit_ws_task
  -> okx_ws_task
  -> bybit_ws_task
  -> hyperliquid_ws_task
  -> rest_snapshot_task
  -> option_gex_refresh_task
  -> normalizer_task
  -> orderbook_task
  -> trade_aggregator_task
  -> feature_task
  -> signal_task
  -> risk_task
  -> execution_task
  -> persistence_task
  -> metrics_task
  -> heartbeat_task
```

Canal:

```text
tokio::sync::mpsc bounded para eventos
tokio::sync::watch para snapshots recentes
arc-swap para feature snapshot hot path
broadcast so para debug/monitoring
```

Regra de backpressure:

- hot path nunca espera disco;
- persistence pode dropar debug, nunca dropar raw critical sem alerta;
- se fila de market data cresce acima limite, bot entra `DEGRADED` e para de operar;
- se atraso nao recupera, resync/reconnect.

Hot path:

- Sem escrita em DB antes de decisao.
- Sem chamada REST para gerar sinal.
- Sem lock pesado.
- Todas features precomputadas incrementalmente.
- Snapshots publicados por atomic pointer ou canal bounded.

Cold path:

- Persistencia parquet.
- Backfill.
- Relatorios.
- Otimizacao.

### 11.2 Loop 24/7

Estados globais:

```text
BOOTING
SYNCING_MARKET_DATA
WARMING_FEATURES
PAPER_READY
TRADING_READY
DEGRADED
KILL_SWITCH
SHUTDOWN
```

Loop principal:

```text
1. carregar config/segredos
2. sincronizar horario
3. conectar streams
4. montar books locais
5. aquecer rolling windows
6. publicar estado READY
7. processar eventos ate shutdown
8. em falha: pausar sinais, cancelar ordens abertas, resync, retomar so depois de checklist
```

Heartbeats:

```yaml
ws_message_timeout_ms: 3000
depth_sequence_gap: hard_resync
trade_stream_stale_ms: 1500
feature_snapshot_stale_ms: 500
gex_stale_seconds: 180
disk_flush_warn_ms: 1000
memory_queue_warn_pct: 75
```

Supervisao:

- reconnect com exponential backoff + jitter;
- circuit breaker por exchange;
- failover de data source secundario para features nao-execucao;
- nunca operar se fonte de execucao principal esta inconsistente;
- kill switch manual via arquivo/env/API local.

Observabilidade:

- `tracing` JSON logs;
- Prometheus metrics;
- health endpoint local;
- dashboard simples depois.

Metricas obrigatorias:

```text
event_lag_ms
ws_reconnect_count
orderbook_resync_count
queue_depth
feature_age_ms
signal_count
blocked_signal_count
paper_pnl_R
slippage_R
unknown_order_state_count
```

### 11.3 Layout de crates

```text
crates/
  core/              tipos, config, clock, math
  adapters/          binance, deribit, okx, bybit, hyperliquid
  orderbook/         local book, heatmap state, queue estimate
  orderflow/         trades, CVD, footprint, absorption/exhaustion
  profile/           TPO, volume profile, VWAP, nPOC
  gex/               options chain, GEX proxy, regime
  features/          feature snapshot incremental
  strategy/          setups, labels, scoring/model interface
  risk/              sizing, kill switch, limits
  execution/         paper broker, binance demo/live adapter
  storage/           parquet/sqlite, replay
  research/          backtest, walk-forward, ablation
  app/               binaries 24/7
```

Inicialmente pode ser workspace unico. Separar crates quando compilacao/ownership pedir.

## 12. Persistencia e replay

Salvar bruto:

- trades normalizados.
- depth diffs.
- depth snapshots.
- option chain/GEX snapshots.
- sinais.
- decisoes.
- ordens/fills.
- latencias.

Formato:

```text
data/
  raw/YYYY-MM-DD/btcusdt/trades.parquet
  raw/YYYY-MM-DD/btcusdt/depth.parquet
  raw/YYYY-MM-DD/options/gex_snapshots.parquet
  runs/<run_id>/signals.parquet
  runs/<run_id>/orders.parquet
```

Replay precisa reproduzir:

- mesmo estado.
- mesmas features.
- mesma decisao.
- mesmo fill model.

Teste obrigatorio:

```text
live_capture -> replay_same_day -> hashes de features/sinais iguais
```

## 13. Backtest e validacao

Fase 1: coleta.

- Minimo 2 semanas live depth/trades/options.
- Ideal 60 dias para composite/TPO e regimes.

Fase 2: replay.

- Sem olhar candle futuro.
- Eventos em ordem real.
- Latencia simulada.
- Fills conservadores.

Fase 3: walk-forward.

- Parametros calibrados em janela A.
- Teste em janela B.
- Repetir.

Metricas:

```yaml
min_trades_before_claiming_edge: 500
expectancy_R: > 0.10
profit_factor: > 1.20
max_drawdown_R: <= 10
avg_slippage_R: < 0.15
timeout_or_unknown_order_rate: < 0.5%
data_desync_rate: < 0.1%
```

Se falhar:

- nao mexer feeling.
- revisar por componente: regime, zona, fluxo, execucao, risco.
- deletar setup ruim se nao sobrevive fora da amostra.

## 14. Parametros iniciais

```yaml
symbol: BTCUSDT
mode: paper_live
timeframes:
  micro_windows_ms: [250, 1000, 5000, 15000, 60000]
profile:
  value_area_pct: 0.70
  bin_atr_fraction: 0.05
gex:
  refresh_seconds: 60
  stale_seconds: 180
  positive_threshold_usd: 1000000
  negative_threshold_usd: -1000000
orderflow:
  aggression_z_1s: 2.0
  aggression_z_5s: 2.5
  absorption_z: 2.0
  replenish_z: 1.5
execution:
  max_spread_ticks: 2
  max_data_age_ms: 300
  entry_ttl_ms: 750
risk:
  starting_capital_brl: 100
  risk_per_trade_brl: 0.50
  max_daily_loss_brl: 2.00
  max_trades_per_day: 8
```

Todos parametros sao hipoteses. Otimizacao so via walk-forward.

## 15. Roadmap

Roadmap por setores. Ordem importa: primeiro dado/replay, depois estrategia. Sem replay confiavel, qualquer edge e miragem.

### Setor A: fundacao Rust + Tokio

Entregaveis:

- workspace Rust;
- config loader;
- logging `tracing`;
- clock/time sync;
- typed events;
- bounded channels;
- supervisor;
- metrics endpoint.

Pronto quando:

- app roda 24h sem trade, so heartbeat;
- reconecta WebSocket fake;
- encerra limpo;
- health mostra `BOOTING/SYNCING/READY/DEGRADED`.

### Setor B: data adapters publicos

Entregaveis:

- Binance USD-M adapter: trades, depth, bookTicker, mark/funding, liquidations.
- MEXC Futures adapter: trades, depth, fair/index price, funding, account/trade se habilitado.
- Binance Options adapter: exchangeInfo, OI, mark greeks.
- Deribit adapter: options ticker/OI/greeks.
- OKX adapter: options/perp OI e greeks conforme endpoints disponiveis.
- Bybit adapter: trades, orderbook, funding/OI/liquidations.
- Hyperliquid adapter: l2Book, trades, BBO, OI/funding.

Pronto quando:

- cada adapter emite eventos normalizados;
- timestamps exchange/local salvos;
- reconnect/resubscribe testado;
- perda/lag gera `DEGRADED`.

### Setor C: storage bruto e replay

Entregaveis:

- writer parquet por tipo de evento;
- rotacao diaria;
- manifest de arquivos;
- replay engine ordenado por timestamp;
- hash deterministico de features.

Pronto quando:

- capturar 24h BTCUSDT;
- replay gerar mesmas features/sinais;
- nenhum sinal depende de dado futuro.

### Setor D: orderbook + heatmap

Entregaveis:

- local orderbook Binance com snapshot+diff correto;
- top-N book;
- heatmap state por nivel;
- wall quality;
- pull/stack;
- spoof score;
- queue estimate.

Pronto quando:

- resync detecta gap;
- wall/pull/stack aparecem em debug;
- absorcao por replenish mensuravel;
- feed stale bloqueia trade.

### Setor E: orderflow/footprint

Entregaveis:

- trade signing;
- CVD multi-window;
- delta z-score;
- footprint por bucket tempo/tick/range;
- volume bubbles;
- absorption/exhaustion/divergence;
- unfinished auction/naked POC de footprint se util.

Pronto quando:

- replay reconstroi candles/footprints;
- eventos do video transcrito podem ser rotulados manualmente;
- metrics mostram delta/price efficiency.

### Setor F: AMT/TPO/VWAP/price action

Entregaveis:

- session manager: Asia/London/NY/UTC/daily/weekly/composite;
- TPO profile;
- volume profile;
- VAH/VAL/POC/VPOC/HVN/LVN/nPOC;
- VWAP session/anchored + bands;
- swings, sweeps, BOS, CHOCH, displacement, FVG/order-block proxy.

Pronto quando:

- perfis batem matematicamente com definicoes TPO/VP e replay interno;
- zones sao reproduziveis em replay;
- price action vira features, nao desenho manual.

### Setor G: GEX proxy

Entregaveis:

- options normalizer multi-exchange;
- GEX proxy por strike/expiry;
- gamma flip proxy;
- max positive/negative gamma levels;
- expiry/OPEX calendar;
- cross-exchange aggregate.

Pronto quando:

- Binance-only, Deribit-only, aggregate comparaveis;
- stale GEX bloqueia setup que depende dele;
- estudo mostra se GEX melhora ou piora setups.

### Setor H: hidden-data proxy engine

Entregaveis:

- hidden liquidity score;
- iceberg/replenishment detector;
- spoof/pull detector;
- stop/liquidity pool map;
- liquidation impulse score;
- crowded positioning proxy;
- cross-exchange lead/lag.

Pronto quando:

- cada proxy tem label e ablation;
- proxy inutil e removido;
- proxy util melhora expectancy fora da amostra.

### Setor I: strategy research

Entregaveis:

- candidate generator: short/long breakout, mean reversion, failed auction;
- labeler: hit 1R/2R, MAE, MFE, return windows de 5s/15s/30s;
- scalp 30s research pack: target, stop, time stop, fee/slippage threshold;
- walk-forward splits;
- ablation AMT vs AMT+flow vs +GEX vs +heatmap;
- threshold optimizer;
- model interface opcional.

Pronto quando:

- 500+ eventos/sinais pesquisados;
- expectancy fora da amostra > custos, principalmente em janela 30s;
- feature importance nao depende de lookahead;
- setup ruim e deletado sem apego.

### Setor J: paper broker R$100

Entregaveis:

- ledger BRL/USDT;
- fill model market/limit;
- fee/slippage;
- fee model por venue: Binance/MEXC/etc;
- liquidation model por venue;
- leverage cap dinamico;
- minQty/tickSize/stepSize;
- risk per trade;
- daily/weekly kill switch.

Pronto quando:

- paper replay e paper live usam mesmo broker;
- ledger fecha PnL por trade;
- slippage e fila estimada registradas.
- leverage alta rejeitada quando liquidation-before-stop aparece.

### Setor K: execution venues

Entregaveis:

- Binance Futures Demo execution adapter;
- MEXC Futures execution adapter se API/regiao/conta permitir;
- signed REST/WebSocket API por venue;
- order placement/cancel;
- user data stream;
- idempotent `clientOrderId`;
- reduce-only stops;
- reconciliation loop.

Pronto quando:

- demo roda sem posicao fantasma;
- timeout nao duplica ordem;
- kill switch cancela/fecha;
- estado local bate estado exchange.
- MEXC fee/risk/leverage lidos do contrato antes da ordem.
- venue sem private stream confiavel fica proibido para live.

### Setor L: 24/7 ops

Entregaveis:

- service runner;
- config profiles: capture, replay, paper_live, demo;
- log rotation;
- metrics dashboard;
- alertas;
- backup de dados;
- restart seguro.

Pronto quando:

- 7 dias capturando sem intervencao;
- reconecta sem corromper replay;
- restart nao perde estado critico;
- DEGRADED pausa trading sozinho.

### Setor M: gate para live

Nao executar live ate:

- 60 dias de dados ou historico pago equivalente;
- 500+ trades paper fora da amostra;
- expectancy positiva apos fees/slippage;
- drawdown dentro do limite;
- kill switch testado;
- min notional nao viola risco R$100;
- usuario aprova explicitamente live.

Sequencia operacional:

```text
capture_only -> replay -> research -> paper_live -> futures_demo -> long paper soak -> live_micro
```

## 16. Criterio de "pronto para automatizar"

Pronto quando:

- Cada setup tem precondicao, gatilho, entrada, stop, alvo e invalidez.
- Cada evento bruto pode ser reprocessado em replay.
- Toda ordem tem causa e snapshot de features.
- Toda perda tem classificacao: tese errada, fluxo falhou, execucao ruim, regime ruim, dado stale.
- Sistema consegue ficar flat sob falha.
- Paper mostra edge depois de custos.

Nao pronto se:

- Sinal depende de olhar grafico.
- GEX e manual.
- Supply/demand desenhada no olho.
- Stop move por medo.
- Target escolhido depois da entrada.

## 17. Mapeamento direto da teoria

| Frase/ideia | Regra automatizada |
|---|---|
| "TPO mostra onde aceita/rejeita" | VAH/VAL/POC + acceptance/rejection state |
| "90% rompimentos falham" | breakout so com GEX negativo + acceptance + fluxo |
| "Gamma positivo amortece" | mean reversion em VA/VWAP bands |
| "Gamma negativo joga gasolina" | breakout por LVN/low/high |
| "Agressao de venda nao empurrou" | sell absorption |
| "Compra nao fez nada" | buy absorption/failure |
| "CVD divergente" | bullish/bearish divergence |
| "Criou supply/demand" | zona criada por agressao falha + agressao oposta |
| "Bookmap mostra ordem segurando" | depth wall + persistencia + replenish |
| "Alvo na liquidez" | prior high/low, round number, wall, nPOC, GEX strike |
| "Sem achismo, provar com dados" | replay, walk-forward, journal, metrics |

## 18. Referencias oficiais usadas

- Binance USD-M Futures General Info: https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info
- Binance USD-M Futures WebSocket Market Streams: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams
- Binance USD-M Futures Aggregate Trade Streams: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
- Binance USD-M Futures Diff Book Depth Streams: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
- Binance USD-M Futures Local Order Book: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
- Binance Options Exchange Information: https://developers.binance.com/docs/derivatives/options-trading/market-data/Exchange-Information
- Binance Options Open Interest: https://developers.binance.com/docs/derivatives/options-trading/market-data/Open-Interest
- Binance Options Mark Price: https://developers.binance.com/docs/derivatives/options-trading/market-data/Option-Mark-Price
- Binance Spot/Futures testing FAQ: https://www.binance.com/en/support/faq/detail/ab78f9a1b8824cf0a106b4229c76496d
- MEXC Futures API change log: https://www.mexc.com/api-docs/futures/update-log
- MEXC Futures market endpoints: https://www.mexc.com/api-docs/futures/market-endpoints/
- MEXC Futures account/trading endpoints: https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/
- MEXC Futures WebSocket API: https://www.mexc.com/api-docs/futures/websocket-api/
- MEXC API fee announcements: https://www.mexc.com/announcements/api-updates
- Deribit API: https://docs.deribit.com/
- Deribit public ticker: https://docs.deribit.com/api-reference/market-data/public-ticker
- Deribit public book summary by currency: https://docs.deribit.com/api-reference/market-data/public-get_book_summary_by_currency
- OKX API v5: https://www.okx.com/docs-v5/en/
- Bybit API: https://www.bybit.com/en/derivative-activity/developer/
- Bybit WebSocket orderbook: https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
- Hyperliquid WebSocket subscriptions: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- Hyperliquid perpetuals info: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
- MMT terminal/features: https://mmt.gg/
- MMT order flow features: https://mmt.gg/features/order-flow
- Amberdata Deribit market data: https://www.amberdata.io/deribit-market-data
