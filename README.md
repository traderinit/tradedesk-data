# tradedesk-data

거래소 **공개** 시세 자료를 모으는 저장소입니다. 비밀값·전략 코드는 없습니다.

| 워크플로 | 내용 | 결과 브랜치 |
|---|---|---|
| okx-daily | OKX 코인 20개 캔들(15m·1H·4H·1D)·시장 구조·펀딩비 — 매일 01:17 UTC | `data` (`okx/`) |
| data-alts | 바이낸스 USDT 무기한 전체(상장 폐지 포함) 1시간봉 — 수동 | `data-alts` |
| data-flow | 바이낸스 선물 미결제약정·롱숏·시장가 매수 — 수동 | `data-flow` |
| data-ext / data-universe / data-lowtf / data-spot | 기타 공개 자료 — 수동 | 각 이름 브랜치 |

출처: OKX 공개 API, data.binance.vision.
