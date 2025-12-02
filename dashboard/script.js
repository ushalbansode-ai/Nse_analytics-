async function loadData() {
    let jsonURL = "../data/signal.json";

    // GitHub Pages support
    if (location.hostname.includes("github.io")) {
        const repo = location.pathname.split("/")[1];
        jsonURL = `https://${location.hostname}/${repo}/data/signal.json`;
    }

    try {
        const res = await fetch(jsonURL + "?v=" + Date.now());
        const data = await res.json();
        renderTable(data);
    } catch (e) {
        console.error("Loading failed", e);
    }
}

function renderTable(data) {
    document.getElementById("updated").innerText = data.timestamp;

    const tbody = document.querySelector("#signalTable tbody");
    tbody.innerHTML = "";

    Object.keys(data.signals).forEach(symbol => {
        const s = data.signals[symbol];

        const row = `
            <tr>
                <td>${symbol}</td>
                <td>${s.trend}</td>
                <td>${s.strength}</td>
                <td>${s.volume_spike}</td>
                <td>${s.reversal_signal}</td>
                <td>${s.premium_discount}</td>
                <td>${s.last_price}</td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

loadData();
// Auto Refresh only during market hours (IST)
setInterval(() => {
    const now = new Date();
    
    // Convert local time to IST
    const istTime = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
    const hours = istTime.getHours();
    const minutes = istTime.getMinutes();
    const day = istTime.getDay();  // 0=Sun, 6=Sat

    const isWeekday = day >= 1 && day <= 5;
    const isMarketHours = (hours > 9 || (hours === 9 && minutes >= 15)) &&
                          (hours < 15 || (hours === 15 && minutes <= 30));

    if (isWeekday && isMarketHours) {
        console.log("Refreshing (market hours IST)...");
        loadData();
    } else {
        console.log("No refresh (market closed)");
    }
}, 5 * 60 * 1000); // 5 minutes



