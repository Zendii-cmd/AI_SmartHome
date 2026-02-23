# ai_advisor.py

def energy_advice(data):
    advice = []

    if data["led1"] and data["led2"] and data["power_mW"] > 2000:
        advice.append("💡 Gợi ý: Nên tắt bớt đèn khi không sử dụng")

    if data["power_mW"] > 2500:
        advice.append("🔋 Gợi ý: Công suất cao, nên kiểm tra thiết bị tiêu thụ")

    if not data["led1"] and not data["led2"] and data["power_mW"] > 1500:
        advice.append("⚠️ AI Nhận xét: Có tải điện nền bất thường")

    return advice
