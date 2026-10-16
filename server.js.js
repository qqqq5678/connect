// server.js (Node.js 백엔드 중계 서버)
const express = require('express');
const { WebSocketServer } = require('ws');
const https = require('https');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// 정적 파일 제공 (클라이언트 HTML)
app.use(express.static(path.join(__dirname, 'public')));

// 방(Room) 상태 관리 (메모리 상에서만 관리하므로 서버 종료 시 자동 소멸)
// 구조: { roomCode: { expireTime: timestamp, clients: Set(ws) } }
const activeRooms = new Map();

wss.on('connection', (ws) => {
    let currentRoom = null;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            // 1. 방 생성 또는 참가 (JOIN)
            if (data.type === 'JOIN') {
                const { room, expireSeconds } = data;
                currentRoom = room;

                if (!activeRooms.has(room)) {
                    // 방이 없으면 새로 생성 (만료 시간 설정)
                    const expireAt = Date.now() + (expireSeconds || 60) * 1000;
                    activeRooms.set(room, {
                        expireAt: expireAt,
                        clients: new Set()
                    });
                }

                const roomData = activeRooms.get(room);
                
                // 만료 시간 체크
                if (Date.now() > roomData.expireAt) {
                    ws.send(JSON.stringify({ type: 'ERROR', message: '만료된 방 코드입니다.' }));
                    return;
                }

                roomData.clients.add(ws);
                ws.send(JSON.stringify({ type: 'JOINED', room: room }));
                console.log(`[+] 사용자 방 입장: ${room} (현재 인원: ${roomData.clients.size}명)`);
            }

            // 2. 메시지 중계 (Relay - 서버는 내용을 보지 않고 암호문 그대로 전달)
            else if (data.type === 'MESSAGE') {
                const { room, encryptedData } = data;
                if (activeRooms.has(room)) {
                    const roomData = activeRooms.get(room);
                    
                    // 방에 속한 다른 클라이언트들에게만 암호문 중계
                    roomData.clients.forEach((client) => {
                        if (client !== ws && client.readyState === WebSocket.OPEN) {
                            client.send(JSON.stringify({
                                type: 'MESSAGE',
                                encryptedData: encryptedData
                            }));
                        }
                    });
                }
            }
        } catch (e) {
            console.error('메시지 파싱 오류:', e);
        }
    });

    ws.on('close', () => {
        if (currentRoom && activeRooms.has(currentRoom)) {
            const roomData = activeRooms.get(currentRoom);
            roomData.clients.delete(ws);
            console.log(`[-] 사용자 방 퇴장: ${currentRoom}`);
            
            // 방에 남은 사람이 없으면 메모리에서 즉시 삭제
            if (roomData.clients.size === 0) {
                activeRooms.delete(currentRoom);
                console.log(`🧹 빈 방 소멸됨: ${currentRoom}`);
            }
        }
    });
});

const PORT = 8080;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 비밀 메신저 중계 서버 실행 중: http://localhost:${PORT}`);
    console.log(`💡 같은 Wi-Fi 환경의 휴대폰에서는 PC의 내부 IP(예: http://192.168.x.x:8080)로 접속하세요.`);
});