package com.mq;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.beans.ABData;

/**
 * 队列，作为接收数据的缓存
 * 
 * @author xzh@bnc.com.cn
 *
 */
public class CircleQueue {

	private int queueSize = 81920000; // 队列大小
	private final long M_WAITTIME = 3000 * 1000000l; // 3000毫秒
	
	private CircleQueueNode m_urlQuene[];
	private final Lock m_qlock;
	private final Condition m_condNotFull; // 条件变量1
	private final Condition m_condNotEmpty; // 条件变量2
	private int m_readPos; // 读位置
	private int m_writePos; // 写位置
	private int m_nodeSize; // 队列中元素的个数
	
	public static void main(String[] args) {
		CircleQueue cq = new CircleQueue(80000000);
		for (int i = 0; i < 80000000; i++) {
			System.out.println(i);
			ABData abData = new ABData();
			CircleQueueNode inNode = new CircleQueueNode(abData);
			cq.WriteQuene(inNode);
		}
	}
	
	public int getNodeSize() {
		return m_nodeSize;
	}
	
	// constructor
	public CircleQueue(int queueSize) {
		this.queueSize = queueSize;
		m_urlQuene = new CircleQueueNode[queueSize];
		m_qlock = new ReentrantLock(); // 互斥锁，这里不能用读写锁
		m_condNotFull = m_qlock.newCondition(); 
		m_condNotEmpty = m_qlock.newCondition();
		m_readPos = 0;
		m_writePos = 0;
	}
	
	/**
	 * 从队列中读取一个节点
	 * @return
	 */
	public CircleQueueNode ReadQuene() {
		CircleQueueNode node = null;
		m_qlock.lock();
		try {
			if(m_readPos == m_writePos) { // 队列为空
//				m_condNotEmpty.await();
				m_condNotEmpty.awaitNanos(M_WAITTIME);
			}
			if(m_readPos == m_writePos) {
//				System.out.println("ReadQuene: timeout!!!["+m_readPos+", "+m_writePos+", "+queueSize+"]");
				return node;
			}
//			node = m_urlQuene.get(m_readPos);
			node = m_urlQuene[m_readPos];
			if(++m_readPos == queueSize) {
				m_readPos = 0;
			}
			m_nodeSize--;
			m_condNotFull.signal();
		} catch (Exception e) {
			System.out.println("*** ReadQuene exception!");
		} finally {
			m_qlock.unlock();
		}
		
		return node;
	}
	
	/**
	 * 向队列中写入一个URL节点
	 * @param inNode
	 */
	public void WriteQuene(CircleQueueNode inNode) {
		m_qlock.lock();
		try {
			if((m_writePos + 1) % queueSize == m_readPos) { // 队列满了
				System.out.println("WriteQuene: quene is full, waiting...");
//				m_condNotFull.await();
				m_condNotFull.awaitNanos(M_WAITTIME);
			}
			if((m_writePos + 1) % queueSize == m_readPos) {
				System.out.println("WriteQuene: timeout!!!");
				return;
			}
			// 向队列中写入节点
//			m_urlQuene.setElementAt(inNode, m_writePos);
			m_urlQuene[m_writePos] = inNode;
			if(++m_writePos == queueSize) {
				m_writePos = 0;
			}
			m_nodeSize++;
			m_condNotEmpty.signal();
		} catch (Exception e) {
			System.out.println("*** WriteQuene exception!");
		} finally {
			m_qlock.unlock();
		}
		
	}
}

