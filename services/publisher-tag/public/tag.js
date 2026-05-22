window.AdTech = (function() {

    // Helper to read the DSP cookie if it exists (Simulated Identity)
    function getCookie(name) {
        let match = document.cookie.match(new RegExp('(^| )' + name + '=([^;]+)'));
        if (match) return match[2];
        return null;
    }

    return {
        requestAd: async function(slotId) {
            const slot = document.getElementById(slotId);
            if (!slot) return;

            // 1. Gather User Context from the browser
            const dspUid = getCookie("dsp_uid") || "";

            // 2. Call our Rust Mock SSP Server
            try {
                const response = await fetch(`/ssp/ad?slot_id=${slotId}&dsp_uid=${dspUid}`);
                const adHtml = await response.text();

                if (adHtml.includes("No Bid Returned") || adHtml.includes("Render Error")) {
                    console.log(`Slot ${slotId} unfilled.`);
                    return;
                }

                // 3. Render inside a secure iframe to prevent advertiser CSS from breaking the publisher site
                const iframe = document.createElement("iframe");
                iframe.width = "300";
                iframe.height = "250";
                iframe.style.border = "none";
                iframe.scrolling = "no";

                slot.appendChild(iframe);

                // Write the HTML payload into the iframe
                const iframeDoc = iframe.contentWindow.document;
                iframeDoc.open();
                iframeDoc.write(adHtml);
                iframeDoc.close();

                console.log(`Ad successfully rendered in ${slotId}`);

            } catch (err) {
                console.error("AdTech Tag Error:", err);
            }
        }
    };
})();