def format_combined_market_status(kospi_cond, kosdaq_cond):
    """KOSPI/KOSDAQ 시장 상태를 하나의 메시지로 결합 (Telegram용)"""
    def get_emoji(regime):
        regime = str(regime).upper()
        if regime == "BULL": return "🚀"
        if regime == "BEAR": return "📉"
        if regime == "CRASH": return "🚨"
        return "⚖️"

    k_regime = kospi_cond.market_regime if hasattr(kospi_cond, "market_regime") else "NEUTRAL"
    q_regime = kosdaq_cond.market_regime if hasattr(kosdaq_cond, "market_regime") else "NEUTRAL"
    k_change = kospi_cond.current_index_change if hasattr(kospi_cond, "current_index_change") else 0.0
    q_change = kosdaq_cond.current_index_change if hasattr(kosdaq_cond, "current_index_change") else 0.0
    k_adr = kospi_cond.adr_ratio if hasattr(kospi_cond, "adr_ratio") else 0.0
    q_adr = kosdaq_cond.adr_ratio if hasattr(kosdaq_cond, "adr_ratio") else 0.0

    msg = (
        f"📊 **Market Pulse**\n"
        f"▫️ KOSPI: {get_emoji(k_regime)} {k_regime} ({k_change:+.2f}%, ADR: {k_adr:.2f})\n"
        f"▫️ KOSDAQ: {get_emoji(q_regime)} {q_regime} ({q_change:+.2f}%, ADR: {q_adr:.2f})"
    )
    return msg
