
const express = require("express");
const http = require("http");
const path = require("path");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 10000;
const rooms = new Map();

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function leaveRoom(ws) {
  if (!ws.room) return;

  const room = rooms.get(ws.room);
  if (!room) return;

  room.clients.delete(ws);

  for (const client of room.clients) {
    send(client, {
      type: "SYSTEM",
      message: "상대방의 연결이 종료되었습니다."
    });
  }

  if (room.clients.size === 0) {
    clearTimeout(room.timer);
    rooms.delete(ws.room);
  }

  ws.room = null;
}

wss.on("connection", (ws) => {
  ws.on("message", (raw) => {
    if (raw.length > 16000) {
      send(ws, {
        type: "ERROR",
        message: "메시지가 너무 큽니다."
      });
      return;
    }

    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      send(ws, {
        type: "ERROR",
        message: "잘못된 요청입니다."
      });
      return;
    }

    if (data.type === "JOIN") {
      const name = String(data.room || "").trim();
      const seconds = [60, 300, 600].includes(
        Number(data.expireSeconds)
      ) ? Number(data.expireSeconds) : 300;

      if (!name || name.length > 80) {
        send(ws, {
          type: "ERROR",
          message: "방 코드를 확인해 주세요."
        });
        return;
      }

      if (ws.room) {
        send(ws, {
          type: "ERROR",
          message: "이미 방에 입장했습니다."
        });
        return;
      }

      let room = rooms.get(name);

      if (!room) {
        room = {
          clients: new Set(),
          expiresAt: Date.now() + seconds * 1000,
          timer: null
        };

        room.timer = setTimeout(() => {
          for (const client of room.clients) {
            send(client, {
              type: "EXPIRED",
              message: "방 사용 시간이 만료되었습니다."
            });
            client.close();
          }

          rooms.delete(name);
        }, seconds * 1000);

        rooms.set(name, room);
      }

      if (Date.now() >= room.expiresAt) {
        send(ws, {
          type: "ERROR",
          message: "방이 만료되었습니다."
        });
        return;
      }

      if (room.clients.size >= 2) {
        send(ws, {
          type: "ERROR",
          message: "방에는 최대 두 명만 입장할 수 있습니다."
        });
        return;
      }

      room.clients.add(ws);
      ws.room = name;

      send(ws, {
        type: "JOINED",
        room: name
      });

      for (const client of room.clients) {
        if (client !== ws) {
          send(client, {
            type: "SYSTEM",
            message: "상대방이 입장했습니다."
          });
        }
      }

      return;
    }

    if (data.type === "MESSAGE") {
      if (!ws.room || data.room !== ws.room) {
        send(ws, {
          type: "ERROR",
          message: "먼저 방에 입장해 주세요."
        });
        return;
      }

      if (
        typeof data.encryptedData !== "string" ||
        data.encryptedData.length > 12000
      ) {
        send(ws, {
          type: "ERROR",
          message: "메시지 형식이 올바르지 않습니다."
        });
        return;
      }

      const room = rooms.get(ws.room);

      if (!room || Date.now() >= room.expiresAt) {
        send(ws, {
          type: "ERROR",
          message: "방이 만료되었습니다."
        });
        return;
      }

      // 메시지를 저장하지 않고 상대방에게만 중계
      for (const client of room.clients) {
        if (client !== ws) {
          send(client, {
            type: "MESSAGE",
            encryptedData: data.encryptedData
          });
        }
      }
    }
  });

  ws.on("close", () => leaveRoom(ws));
  ws.on("error", () => leaveRoom(ws));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Server listening on port ${PORT}`);
});
