FROM busybox

WORKDIR /gemini

COPY gemini .

ENV PATH="/gemini:${PATH}"

ENTRYPOINT ["gemini"]
