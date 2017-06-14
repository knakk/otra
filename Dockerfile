FROM alpine
RUN apk add --update --no-cache ca-certificates
ADD otra /otra
CMD ["/otra"]
